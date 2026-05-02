"""
padStream - Gamepad & Audio Streaming
Single-file application with system tray support.

Dependencies:
    pip install pygame pyaudiowpatch vgamepad PyQt6 pywin32

Server mode: receives gamepad input + audio, emulates virtual controller, plays audio
Client mode: captures gamepad + system audio, streams to server
"""

import sys
import os
import json
import time
import math
import socket
import struct
import threading
import queue
import logging
from pathlib import Path
from typing import Optional

# ── Qt must be imported before anything that touches the event loop ──────────
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QComboBox, QStackedWidget, QFrame,
    QSystemTrayIcon, QMenu, QGraphicsDropShadowEffect, QSizePolicy,
    QScrollArea, QTabWidget,
)
from PyQt6.QtCore import (
    Qt, QTimer, QThread, QObject, pyqtSignal, QPropertyAnimation,
    QEasingCurve, QSize, QPoint, QSettings,
)
from PyQt6.QtGui import (
    QColor, QFont, QPainter, QPen, QBrush, QLinearGradient,
    QRadialGradient, QIcon, QPixmap, QPalette, QFontDatabase,
    QAction,
)

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("padstream")

# ── Config paths ─────────────────────────────────────────────────────────────
CONFIG_DIR = Path(os.getenv("APPDATA", Path.home())) / "padStream"
CONFIG_FILE = CONFIG_DIR / "settings.json"
CONFIG_DIR.mkdir(parents=True, exist_ok=True)

# ── Protocol constants ────────────────────────────────────────────────────────
DISCOVERY_PORT   = 57421
CONTROL_PORT     = 57422
AUDIO_PORT       = 57423
DISCOVERY_MAGIC  = b"GS_DISCOVER_V1"
DISCOVERY_REPLY  = b"GS_HERE_V1"

AUDIO_SAMPLE_RATE = 48000
AUDIO_CHANNELS    = 2
AUDIO_CHUNK       = 256          # ~5.3 ms @ 48 kHz
AUDIO_QUEUE_MAX   = 4            # ~20 ms buffer

PKT_GAMEPAD    = 0x01
PKT_AUDIO      = 0x02
PKT_PING       = 0x03
PKT_PONG       = 0x04
PKT_RUMBLE     = 0x05   # server → client: large_motor, small_motor (0-255)

GAMEPAD_STRUCT = "!BdHBBhhhh"   # type, ts, btns, lt, rt, lx, ly, rx, ry
GAMEPAD_SIZE   = struct.calcsize(GAMEPAD_STRUCT)

# ── Default settings ──────────────────────────────────────────────────────────
DEFAULT_SETTINGS = {
    "mode":           "client",
    "auto_connect":   True,
    "server_ip":      "",
    "capture_device": "",
    "output_device":  "",
    "gamepad_index":  -1,
    "send_audio":     True,            # client: stream audio to server
}


def load_settings() -> dict:
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            s = json.load(f)
        return {**DEFAULT_SETTINGS, **s}
    except Exception:
        return dict(DEFAULT_SETTINGS)


def save_settings(s: dict):
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(s, f, indent=2)
    except Exception as e:
        log.warning("Could not save settings: %s", e)


# ═════════════════════════════════════════════════════════════════════════════
# PROTOCOL HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def pack_gamepad(buttons, lt, rt, lx, ly, rx, ry) -> bytes:
    return struct.pack(GAMEPAD_STRUCT,
                       PKT_GAMEPAD, time.time(),
                       buttons, lt, rt, lx, ly, rx, ry)


def unpack_gamepad(data: bytes) -> dict:
    t, ts, btns, lt, rt, lx, ly, rx, ry = struct.unpack_from(GAMEPAD_STRUCT, data)
    return dict(ts=ts, buttons=btns, lt=lt, rt=rt, lx=lx, ly=ly, rx=rx, ry=ry)


# Audio packet: type(1) + sr(4) + ch(1) + pcm(...)
AUDIO_HDR_STRUCT = "!BIB"
AUDIO_HDR_SIZE   = struct.calcsize(AUDIO_HDR_STRUCT)

def pack_audio(pcm: bytes, sr: int, ch: int) -> bytes:
    return struct.pack(AUDIO_HDR_STRUCT, PKT_AUDIO, sr, ch) + pcm


def unpack_audio(data: bytes):
    """Returns (sr, ch, pcm_bytes)"""
    _, sr, ch = struct.unpack_from(AUDIO_HDR_STRUCT, data)
    return sr, ch, data[AUDIO_HDR_SIZE:]


# ═════════════════════════════════════════════════════════════════════════════
# DISCOVERY SERVICE  (UDP broadcast)
# ═════════════════════════════════════════════════════════════════════════════

class DiscoveryServer(QThread):
    """Server-side: listens for broadcasts, replies with our IP."""

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", DISCOVERY_PORT))
        sock.settimeout(1.0)
        log.info("Discovery server listening on :%d", DISCOVERY_PORT)
        while not self.isInterruptionRequested():
            try:
                data, addr = sock.recvfrom(256)
                if data == DISCOVERY_MAGIC:
                    sock.sendto(DISCOVERY_REPLY, addr)
                    log.info("Discovery ping from %s", addr[0])
            except socket.timeout:
                pass
        sock.close()


class DiscoveryClient(QThread):
    """Client-side: broadcasts, collects server IPs."""
    found = pyqtSignal(list)   # list of IP strings

    def run(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.settimeout(2.0)
        servers = []
        try:
            sock.sendto(DISCOVERY_MAGIC, ("<broadcast>", DISCOVERY_PORT))
            deadline = time.time() + 2.0
            while time.time() < deadline:
                try:
                    data, addr = sock.recvfrom(256)
                    if data == DISCOVERY_REPLY and addr[0] not in servers:
                        servers.append(addr[0])
                except socket.timeout:
                    break
        except Exception as e:
            log.warning("Discovery error: %s", e)
        finally:
            sock.close()
        self.found.emit(servers)


# ═════════════════════════════════════════════════════════════════════════════
# GAMEPAD CAPTURE  (pygame – client side)
# ═════════════════════════════════════════════════════════════════════════════

class GamepadCapture(QThread):
    """Polls pygame joysticks, emits raw gamepad state."""
    state_changed = pyqtSignal(dict)

    def __init__(self, device_index: int = -1):
        super().__init__()
        self.device_index = device_index
        self._active_index = -1

    def run(self):
        try:
            import pygame
        except ImportError:
            log.error("pygame not installed")
            return

        os.environ.setdefault("SDL_VIDEODRIVER", "dummy")
        os.environ.setdefault("SDL_AUDIODRIVER", "dummy")
        pygame.init()
        pygame.joystick.init()

        last_state = None
        js: Optional[pygame.joystick.Joystick] = None

        while not self.isInterruptionRequested():
            pygame.event.pump()
            count = pygame.joystick.get_count()

            # (Re)select joystick
            if js is None or not js.get_init():
                js = self._pick_joystick(pygame, count)

            if js is None:
                time.sleep(0.5)
                continue

            state = self._read_state(pygame, js)
            if state != last_state:
                self.state_changed.emit(state)
                last_state = state
            time.sleep(0.004)   # ~250 Hz poll

        pygame.quit()

    def _pick_joystick(self, pygame, count):
        if count == 0:
            return None
        if self.device_index >= 0 and self.device_index < count:
            idx = self.device_index
        else:
            # Auto: pick first one that has any axis movement
            idx = 0
        try:
            js = pygame.joystick.Joystick(idx)
            self._active_index = idx
            log.info("Gamepad selected: [%d] %s", idx, js.get_name())
            return js
        except Exception as e:
            log.warning("Joystick init error: %s", e)
            return None

    @staticmethod
    def _read_state(pygame, js) -> dict:
        axes    = [js.get_axis(i)   for i in range(js.get_numaxes())]
        buttons = [js.get_button(i) for i in range(js.get_numbuttons())]
        hats    = [js.get_hat(i)    for i in range(js.get_numhats())]
        return {"axes": axes, "buttons": buttons, "hats": hats,
                "name": js.get_name()}

    @staticmethod
    def list_gamepads() -> list:
        try:
            import pygame
            os.environ.setdefault("SDL_VIDEODRIVER", "dummy")
            os.environ.setdefault("SDL_AUDIODRIVER", "dummy")
            if not pygame.get_init():
                pygame.init()
            pygame.joystick.init()
            result = []
            for i in range(pygame.joystick.get_count()):
                js = pygame.joystick.Joystick(i)
                result.append((i, js.get_name()))
            return result
        except Exception as e:
            log.warning("list_gamepads: %s", e)
            return []


def pygame_state_to_xinput(state: dict) -> dict:
    """Convert pygame joystick state to XInput-style values."""
    axes    = state.get("axes", [])
    buttons = state.get("buttons", [])
    hats    = state.get("hats", [])

    def axis(i, default=0.0):
        return axes[i] if i < len(axes) else default

    def btn(i):
        return bool(buttons[i]) if i < len(buttons) else False

    # Map axes: 0=LX, 1=LY, 2=RX, 3=RY, 4=LT, 5=RT  (XInput layout)
    lx = int(axis(0) * 32767)
    ly = int(-axis(1) * 32767)   # invert Y
    rx = int(axis(2) * 32767)
    ry = int(-axis(3) * 32767)
    lt = int(max(0, axis(4)) * 255) if len(axes) > 4 else 0
    rt = int(max(0, axis(5)) * 255) if len(axes) > 5 else 0

    # Buttons (XInput order: A=0,B=1,X=2,Y=3,LB=4,RB=5,Back=6,Start=7,LS=8,RS=9,Guide=10)
    btns = 0
    mapping = [
        (0,  0x1000),  # A
        (1,  0x2000),  # B
        (2,  0x4000),  # X
        (3,  0x8000),  # Y
        (4,  0x0100),  # LB
        (5,  0x0200),  # RB
        (6,  0x0020),  # Back
        (7,  0x0010),  # Start
        (8,  0x0040),  # LS
        (9,  0x0080),  # RS
        (10, 0x0400),  # Guide
    ]
    for idx, mask in mapping:
        if btn(idx):
            btns |= mask

    # D-Pad from hat
    if hats:
        hx, hy = hats[0]
        if hy >  0: btns |= 0x0001   # Up
        if hy < 0:  btns |= 0x0002   # Down
        if hx < 0:  btns |= 0x0004   # Left
        if hx >  0: btns |= 0x0008   # Right

    return dict(buttons=btns, lt=lt, rt=rt, lx=lx, ly=ly, rx=rx, ry=ry)


# ═════════════════════════════════════════════════════════════════════════════
# AUDIO CAPTURE  (PyAudioWPatch loopback – client side)
# ═════════════════════════════════════════════════════════════════════════════

class AudioCapture(QThread):
    """Captures system loopback audio and calls callback directly (no Qt signal overhead)."""

    def __init__(self, device_name: str = "", callback=None):
        super().__init__()
        self.device_name = device_name
        self._callback = callback

    def set_callback(self, cb):
        self._callback = cb

    def run(self):
        try:
            import pyaudiowpatch as pyaudio
        except ImportError:
            log.error("pyaudiowpatch not installed")
            return

        # Boost thread priority
        try:
            import ctypes
            ctypes.windll.avrt.AvSetMmThreadCharacteristicsW("Pro Audio", ctypes.byref(ctypes.c_ulong(0)))
        except Exception:
            pass

        p = pyaudio.PyAudio()
        device = self._pick_device(p, self.device_name)
        if device is None:
            log.error("No loopback device found")
            p.terminate()
            return

        dev_info = p.get_device_info_by_index(device)
        sr = int(dev_info["defaultSampleRate"])
        ch = min(int(dev_info["maxInputChannels"]), 2)
        log.info("Audio capture: %s @ %d Hz x%d ch chunk=%d", dev_info["name"], sr, ch, AUDIO_CHUNK)

        try:
            stream = p.open(
                format=pyaudio.paFloat32,
                channels=ch,
                rate=sr,
                input=True,
                frames_per_buffer=AUDIO_CHUNK,
                input_device_index=device,
            )
        except Exception as e:
            log.error("Audio open error: %s", e)
            p.terminate()
            return

        log.info("Audio capture started")
        while not self.isInterruptionRequested():
            try:
                pcm = stream.read(AUDIO_CHUNK, exception_on_overflow=False)
                if pcm and self._callback:
                    self._callback(pcm, sr, ch)
            except Exception as e:
                log.warning("Audio read error: %s", e)
                break

        stream.stop_stream()
        stream.close()
        p.terminate()
        log.info("Audio capture stopped")

    @staticmethod
    def _pick_device(p, name: str) -> Optional[int]:
        try:
            import pyaudiowpatch as pyaudio
        except ImportError:
            return None

        try:
            # Try wasapi loopback default first
            default_speakers = p.get_default_wasapi_loopback()
            if not name:
                return default_speakers["index"]
        except Exception:
            pass

        # Search by name substring
        for i in range(p.get_device_count()):
            info = p.get_device_info_by_index(i)
            is_loopback = info.get("isLoopbackDevice", False)
            if is_loopback and (not name or name.lower() in info["name"].lower()):
                return i
        return None

    @staticmethod
    def list_loopback_devices() -> list:
        try:
            import pyaudiowpatch as pyaudio
            p = pyaudio.PyAudio()
            devices = []
            for i in range(p.get_device_count()):
                info = p.get_device_info_by_index(i)
                if info.get("isLoopbackDevice", False):
                    devices.append((i, info["name"]))
            p.terminate()
            return devices
        except Exception as e:
            log.warning("list_loopback_devices: %s", e)
            return []


# ═════════════════════════════════════════════════════════════════════════════
# AUDIO PLAYBACK  (PyAudioWPatch – server side)
# ═════════════════════════════════════════════════════════════════════════════

class AudioPlayback:
    """
    Glitch-free playback via ring buffer + PortAudio callback.
    push() writes bytes into the ring from any thread.
    PortAudio callback reads from the ring — no blocking on hot path.
    """

    RING_CHUNKS = 8   # ring holds this many AUDIO_CHUNKs (~40 ms @ 48kHz)

    def __init__(self, device_name: str = ""):
        self.device_name = device_name
        self._stop   = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._lock   = threading.Lock()
        # Ring buffer state — (re)initialized on first push
        self._ring:      bytearray = bytearray()
        self._ring_size: int = 0
        self._wp: int = 0
        self._rp: int = 0
        self._cur_sr: int = 0
        self._cur_ch: int = 0

    def start(self):
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3)

    def push(self, pcm: bytes, sr: int, ch: int):
        with self._lock:
            # Reinit ring on format change
            if sr != self._cur_sr or ch != self._cur_ch:
                chunk_b   = AUDIO_CHUNK * ch * 4
                ring_size = chunk_b * self.RING_CHUNKS
                self._ring      = bytearray(ring_size)
                self._ring_size = ring_size
                self._wp = self._rp = 0
                self._cur_sr = sr
                self._cur_ch = ch

            ring = self._ring
            size = self._ring_size
            wp   = self._wp
            for b in pcm:
                ring[wp] = b
                wp = (wp + 1) % size
            # If write laps read, advance read (drop oldest)
            if wp == self._rp:
                chunk_b = AUDIO_CHUNK * self._cur_ch * 4
                self._rp = (self._rp + chunk_b) % size
            self._wp = wp

    def _read(self, n: int, ring, size, silence) -> bytes:
        """Read n bytes from ring. Returns silence on underrun. Must hold lock."""
        rp = self._rp
        wp = self._wp
        avail = (wp - rp) % size if wp >= rp else size - rp + wp
        if avail < n:
            return silence
        out = bytearray(n)
        for i in range(n):
            out[i] = ring[rp]
            rp = (rp + 1) % size
        self._rp = rp
        return bytes(out)

    def _run(self):
        try:
            import pyaudiowpatch as pyaudio
        except ImportError:
            log.error("pyaudiowpatch not installed")
            return

        try:
            import ctypes
            ctypes.windll.avrt.AvSetMmThreadCharacteristicsW(
                "Pro Audio", ctypes.byref(ctypes.c_ulong(0)))
        except Exception:
            pass

        p = pyaudio.PyAudio()
        device = self._pick_output(p, self.device_name)
        if device is None:
            try:
                device = p.get_default_output_device_info()["index"]
            except Exception:
                log.error("No output device")
                p.terminate()
                return

        dev_info = p.get_device_info_by_index(device)
        log.info("Audio playback: %s", dev_info["name"])

        stream      = None
        opened_sr   = opened_ch = 0

        while not self._stop.is_set():
            sr = self._cur_sr
            ch = self._cur_ch
            if sr == 0:
                time.sleep(0.02)
                continue

            if sr != opened_sr or ch != opened_ch:
                if stream:
                    try: stream.stop_stream(); stream.close()
                    except Exception: pass
                    stream = None

                chunk_b = AUDIO_CHUNK * ch * 4
                sil     = b'\x00' * chunk_b

                def _make_cb(cb, cs, sil_=sil):
                    def _cb(in_data, frame_count, time_info, status):
                        if self._stop.is_set():
                            return (sil_, pyaudio.paComplete)
                        with self._lock:
                            data = self._read(cs, self._ring,
                                              self._ring_size, sil_)
                        return (data, pyaudio.paContinue)
                    return _cb

                try:
                    stream = p.open(
                        format=pyaudio.paFloat32,
                        channels=ch,
                        rate=sr,
                        output=True,
                        frames_per_buffer=AUDIO_CHUNK,
                        output_device_index=device,
                        stream_callback=_make_cb(None, chunk_b),
                    )
                    stream.start_stream()
                    opened_sr, opened_ch = sr, ch
                    log.info("Playback stream: %d Hz x%d ch", sr, ch)
                except Exception as e:
                    log.error("Playback open error: %s", e)
                    time.sleep(0.5)
                    continue

            time.sleep(0.05)

        if stream:
            try: stream.stop_stream(); stream.close()
            except Exception: pass
        p.terminate()
        log.info("Audio playback stopped")

    @staticmethod
    def _pick_output(p, name: str) -> Optional[int]:
        if not name:
            try:
                return p.get_default_output_device_info()["index"]
            except Exception:
                return None
        for i in range(p.get_device_count()):
            info = p.get_device_info_by_index(i)
            if info["maxOutputChannels"] > 0 and name.lower() in info["name"].lower():
                return i
        return None

    @staticmethod
    def list_output_devices() -> list:
        try:
            import pyaudiowpatch as pyaudio
            p = pyaudio.PyAudio()
            devices = []
            for i in range(p.get_device_count()):
                info = p.get_device_info_by_index(i)
                if info["maxOutputChannels"] > 0:
                    devices.append((i, info["name"]))
            p.terminate()
            return devices
        except Exception as e:
            log.warning("list_output_devices: %s", e)
            return []



# ═════════════════════════════════════════════════════════════════════════════
# VIRTUAL GAMEPAD  (vgamepad – server side)
# ═════════════════════════════════════════════════════════════════════════════

class VirtualGamepad:
    def __init__(self):
        self._pad = None
        self._last_error = ""
        self.rumble_callback = None   # called with (large: int, small: int) 0-255

    def start(self) -> bool:
        try:
            if getattr(sys, "frozen", False):
                dll_path = Path(sys._MEIPASS) / "vgamepad" / "win" / "vigem" / "client" / "x64" / "ViGEmClient.dll"
                if dll_path.exists():
                    import ctypes
                    ctypes.WinDLL(str(dll_path))
                    log.info("Pre-loaded ViGEmClient.dll from %s", dll_path)

            import vgamepad as vg
            self._pad = vg.VX360Gamepad()

            # Register rumble notification
            self._pad.register_notification(self._on_rumble)
            log.info("Virtual gamepad created with rumble support")
            return True
        except Exception as e:
            log.error("VirtualGamepad start error: %s", e)
            if self._try_install_vigem():
                try:
                    import vgamepad as vg
                    self._pad = vg.VX360Gamepad()
                    self._pad.register_notification(self._on_rumble)
                    log.info("Virtual gamepad created after ViGEmBus install")
                    return True
                except Exception as e2:
                    log.error("VirtualGamepad still failing after install: %s", e2)
                    self._last_error = f"{e2} (driver installed, reboot may be required)"
                    return False
            self._last_error = str(e)
            return False

    def _on_rumble(self, client, target, large_motor, small_motor, led_number, user_data):
        """Called by ViGEm when a game sends rumble to the virtual controller."""
        log.debug("Rumble: large=%d small=%d", large_motor, small_motor)
        if self.rumble_callback:
            try:
                self.rumble_callback(int(large_motor), int(small_motor))
            except Exception as e:
                log.warning("Rumble callback error: %s", e)

    @staticmethod
    def _is_vigem_installed() -> bool:
        """Check registry for ViGEmBus driver."""
        try:
            import winreg
            paths = [
                r"SYSTEM\CurrentControlSet\Services\ViGEmBus",
                r"SOFTWARE\Nefarius Software Solutions e.U.\ViGEmBus Driver",
                r"SOFTWARE\WOW6432Node\Nefarius Software Solutions e.U.\ViGEmBus Driver",
            ]
            for path in paths:
                for hive in (winreg.HKEY_LOCAL_MACHINE,):
                    try:
                        winreg.OpenKey(hive, path)
                        return True
                    except OSError:
                        pass
        except Exception:
            pass
        return False

    @staticmethod
    def _try_install_vigem() -> bool:
        """Look for ViGEmBus installer — bundled inside exe or next to script."""
        import subprocess

        # Skip if already installed
        if VirtualGamepad._is_vigem_installed():
            log.info("ViGEmBus already installed, skipping installer")
            return False   # already installed but DLL still failed — different problem

        base_dirs = [
            Path(getattr(sys, "_MEIPASS", "")),    # PyInstaller extracted files
            Path(sys.executable).parent,            # PyInstaller exe dir
            Path(__file__).parent,                  # script dir
            Path.cwd(),                             # current working dir
        ]

        installer = None
        for d in base_dirs:
            if not d or not d.exists():
                continue
            matches = list(d.glob("ViGEmBus*.exe"))
            if matches:
                installer = matches[0]
                break

        if not installer:
            log.warning("ViGEmBus installer not found")
            return False

        log.info("Running ViGEmBus installer: %s", installer)
        try:
            result = subprocess.run(
                [str(installer), "/install", "/quiet", "/norestart"],
                timeout=60,
                capture_output=True,
            )
            log.info("ViGEmBus installer exit code: %d", result.returncode)
            return result.returncode in (0, 3010)
        except subprocess.TimeoutExpired:
            log.error("ViGEmBus installer timed out")
            return False
        except Exception as e:
            log.error("ViGEmBus installer error: %s", e)
            return False

    def stop(self):
        self._pad = None

    def apply(self, state: dict):
        if self._pad is None:
            return
        try:
            import vgamepad as vg
            pad = self._pad
            pad.left_joystick_float(
                x_value_float=max(-1.0, min(1.0, state["lx"] / 32767)),
                y_value_float=max(-1.0, min(1.0, state["ly"] / 32767)),
            )
            pad.right_joystick_float(
                x_value_float=max(-1.0, min(1.0, state["rx"] / 32767)),
                y_value_float=max(-1.0, min(1.0, state["ry"] / 32767)),
            )
            pad.left_trigger(value=state["lt"])
            pad.right_trigger(value=state["rt"])
            btns = state["buttons"]
            btn_map = {
                vg.XUSB_BUTTON.XUSB_GAMEPAD_A:              0x1000,
                vg.XUSB_BUTTON.XUSB_GAMEPAD_B:              0x2000,
                vg.XUSB_BUTTON.XUSB_GAMEPAD_X:              0x4000,
                vg.XUSB_BUTTON.XUSB_GAMEPAD_Y:              0x8000,
                vg.XUSB_BUTTON.XUSB_GAMEPAD_LEFT_SHOULDER:  0x0100,
                vg.XUSB_BUTTON.XUSB_GAMEPAD_RIGHT_SHOULDER: 0x0200,
                vg.XUSB_BUTTON.XUSB_GAMEPAD_BACK:           0x0020,
                vg.XUSB_BUTTON.XUSB_GAMEPAD_START:          0x0010,
                vg.XUSB_BUTTON.XUSB_GAMEPAD_LEFT_THUMB:     0x0040,
                vg.XUSB_BUTTON.XUSB_GAMEPAD_RIGHT_THUMB:    0x0080,
                vg.XUSB_BUTTON.XUSB_GAMEPAD_GUIDE:          0x0400,
                vg.XUSB_BUTTON.XUSB_GAMEPAD_DPAD_UP:        0x0001,
                vg.XUSB_BUTTON.XUSB_GAMEPAD_DPAD_DOWN:      0x0002,
                vg.XUSB_BUTTON.XUSB_GAMEPAD_DPAD_LEFT:      0x0004,
                vg.XUSB_BUTTON.XUSB_GAMEPAD_DPAD_RIGHT:     0x0008,
            }
            for vg_btn, mask in btn_map.items():
                if btns & mask:
                    pad.press_button(vg_btn)
                else:
                    pad.release_button(vg_btn)
            pad.update()
        except Exception as e:
            log.warning("VirtualGamepad apply error: %s", e)


# ═════════════════════════════════════════════════════════════════════════════
# NETWORK WORKERS
# ═════════════════════════════════════════════════════════════════════════════

class StreamSender(QThread):
    """Client → Server: sends gamepad packets over TCP, audio over UDP."""
    status_changed = pyqtSignal(str)
    latency_update = pyqtSignal(float)

    def __init__(self, server_ip: str):
        super().__init__()
        self.server_ip = server_ip
        self._gamepad_queue: queue.Queue = queue.Queue(maxsize=16)
        # UDP socket created once, reused by push_audio called from capture thread
        self._udp: Optional[socket.socket] = None
        self._udp_lock = threading.Lock()

    def _apply_rumble(self, large: int, small: int):
        """Apply rumble to the physical gamepad via pygame/SDL."""
        try:
            import pygame
            js_count = pygame.joystick.get_count()
            if js_count == 0:
                return
            # Apply to all connected joysticks (or just index 0)
            for i in range(min(js_count, 1)):
                js = pygame.joystick.Joystick(i)
                # SDL2 rumble: low_freq, high_freq, duration_ms
                low  = int(large / 255 * 65535)
                high = int(small / 255 * 65535)
                js.rumble(low / 65535, high / 65535, 250)
            log.debug("Rumble applied: large=%d small=%d", large, small)
        except Exception as e:
            log.debug("Rumble apply error: %s", e)

    def push_gamepad(self, state: dict):
        xi = pygame_state_to_xinput(state)
        pkt = pack_gamepad(**xi)
        try:
            self._gamepad_queue.put_nowait(pkt)
        except queue.Full:
            try:
                self._gamepad_queue.get_nowait()
                self._gamepad_queue.put_nowait(pkt)
            except Exception:
                pass

    def push_audio(self, pcm: bytes, sr: int, ch: int):
        """Called directly from AudioCapture thread — send immediately, no queue."""
        with self._udp_lock:
            udp = self._udp
        if udp is None:
            return
        try:
            pkt = pack_audio(pcm, sr, ch)
            udp.sendto(pkt, (self.server_ip, AUDIO_PORT))
        except Exception:
            pass

    def run(self):
        while not self.isInterruptionRequested():
            try:
                self._connect_and_stream()
            except Exception as e:
                log.warning("StreamSender error: %s", e)
                self.status_changed.emit(f"Error: {e}")
            if not self.isInterruptionRequested():
                time.sleep(3)
                self.status_changed.emit("Reconnecting…")

    def _connect_and_stream(self):
        # TCP for gamepad
        tcp = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        tcp.settimeout(5)
        tcp.connect((self.server_ip, CONTROL_PORT))
        tcp.settimeout(None)   # blocking — recv thread handles reads

        # UDP for audio
        udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        udp.setsockopt(socket.IPPROTO_IP, socket.IP_TOS, 0x10)
        with self._udp_lock:
            self._udp = udp

        self.status_changed.emit("connected")
        log.info("StreamSender connected to %s", self.server_ip)

        # Receive thread: handles PKT_PONG and PKT_RUMBLE from server
        def recv_loop():
            buf = b""
            try:
                while not self.isInterruptionRequested():
                    try:
                        chunk = tcp.recv(4096)
                        if not chunk:
                            break
                        buf += chunk
                        while len(buf) >= 4:
                            pkt_len = struct.unpack_from("!I", buf)[0]
                            if len(buf) < 4 + pkt_len:
                                break
                            pkt = buf[4:4 + pkt_len]
                            buf = buf[4 + pkt_len:]
                            if not pkt:
                                continue
                            ptype = pkt[0]
                            if ptype == PKT_PONG:
                                ts = struct.unpack_from("!d", pkt, 1)[0]
                                self.latency_update.emit((time.time() - ts) * 1000)
                            elif ptype == PKT_RUMBLE:
                                large = pkt[1] if len(pkt) > 1 else 0
                                small = pkt[2] if len(pkt) > 2 else 0
                                self._apply_rumble(large, small)
                    except OSError:
                        break
            except Exception as e:
                log.warning("recv_loop error: %s", e)

        recv_thread = threading.Thread(target=recv_loop, daemon=True)
        recv_thread.start()

        try:
            while not self.isInterruptionRequested():
                try:
                    pkt = self._gamepad_queue.get(timeout=0.3)
                    tcp.sendall(struct.pack("!I", len(pkt)) + pkt)
                except queue.Empty:
                    # Send ping
                    ping = struct.pack("!BdI", PKT_PING, time.time(), 0)
                    try:
                        tcp.sendall(struct.pack("!I", len(ping)) + ping)
                    except OSError:
                        break
        finally:
            with self._udp_lock:
                self._udp = None
            self.status_changed.emit("disconnected")
            tcp.close()
            udp.close()


class StreamReceiver(QThread):
    """Server side: accepts TCP gamepad + UDP audio, dispatches to handlers."""
    gamepad_received    = pyqtSignal(dict)
    client_connected    = pyqtSignal(str)
    client_disconnected = pyqtSignal()
    latency_update      = pyqtSignal(float)

    def __init__(self):
        super().__init__()
        self.audio_callback = None
        self._conn: Optional[socket.socket] = None
        self._conn_lock = threading.Lock()

    def send_rumble(self, large: int, small: int):
        """Send rumble packet back to client over the active TCP connection."""
        pkt = struct.pack("!BBB", PKT_RUMBLE, large & 0xFF, small & 0xFF)
        with self._conn_lock:
            conn = self._conn
        if conn:
            try:
                conn.sendall(struct.pack("!I", len(pkt)) + pkt)
            except Exception as e:
                log.warning("Rumble send error: %s", e)

    def run(self):
        tcp_thread = threading.Thread(target=self._tcp_server, daemon=True)
        udp_thread = threading.Thread(target=self._udp_server, daemon=True)
        tcp_thread.start()
        udp_thread.start()
        while not self.isInterruptionRequested():
            time.sleep(0.5)

    def _tcp_server(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("", CONTROL_PORT))
        srv.listen(1)
        srv.settimeout(1.0)
        log.info("TCP gamepad server on :%d", CONTROL_PORT)
        while not self.isInterruptionRequested():
            try:
                conn, addr = srv.accept()
                log.info("Client connected: %s", addr[0])
                self.client_connected.emit(addr[0])
                self._handle_client(conn)
                self.client_disconnected.emit()
                log.info("Client disconnected")
            except socket.timeout:
                pass
        srv.close()

    def _handle_client(self, conn: socket.socket):
        conn.settimeout(1.0)
        with self._conn_lock:
            self._conn = conn
        buf = b""
        try:
            while not self.isInterruptionRequested():
                try:
                    chunk = conn.recv(4096)
                    if not chunk:
                        break
                    buf += chunk
                    while len(buf) >= 4:
                        pkt_len = struct.unpack_from("!I", buf)[0]
                        if len(buf) < 4 + pkt_len:
                            break
                        pkt = buf[4:4 + pkt_len]
                        buf = buf[4 + pkt_len:]
                        self._dispatch_tcp(conn, pkt)
                except socket.timeout:
                    continue
        finally:
            with self._conn_lock:
                self._conn = None
            conn.close()

    def _dispatch_tcp(self, conn, pkt: bytes):
        if not pkt:
            return
        ptype = pkt[0]
        if ptype == PKT_GAMEPAD:
            state = unpack_gamepad(pkt)
            self.gamepad_received.emit(state)
        elif ptype == PKT_PING:
            ts = struct.unpack_from("!d", pkt, 1)[0]
            lat = (time.time() - ts) * 1000
            self.latency_update.emit(lat)
            pong = struct.pack("!Bd", PKT_PONG, ts)
            conn.sendall(struct.pack("!I", len(pong)) + pong)

    def _udp_server(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("", AUDIO_PORT))
        sock.settimeout(1.0)
        log.info("UDP audio server on :%d", AUDIO_PORT)
        while not self.isInterruptionRequested():
            try:
                data, _ = sock.recvfrom(65536)
                if data and data[0] == PKT_AUDIO:
                    sr, ch, pcm = unpack_audio(data)
                    if self.audio_callback and pcm:
                        self.audio_callback(pcm, sr, ch)
            except socket.timeout:
                pass
        sock.close()


# ═════════════════════════════════════════════════════════════════════════════
# ORCHESTRATOR  (ties all pieces together)
# ═════════════════════════════════════════════════════════════════════════════

class Orchestrator(QObject):
    """High-level state machine that starts/stops all subsystems."""
    status_changed    = pyqtSignal(str)
    latency_update    = pyqtSignal(float)
    client_changed    = pyqtSignal(str)   # server: client IP or ""
    gamepad_activity  = pyqtSignal()      # blink indicator

    def __init__(self, settings: dict):
        super().__init__()
        self.settings = settings
        self._running = False

        # Subsystems
        self._discovery_srv:   Optional[DiscoveryServer]  = None
        self._disc:            Optional[DiscoveryClient]  = None
        self._gamepad_cap:     Optional[GamepadCapture]   = None
        self._audio_cap:       Optional[AudioCapture]     = None
        self._sender:          Optional[StreamSender]     = None
        self._receiver:        Optional[StreamReceiver]   = None
        self._vpad:            Optional[VirtualGamepad]   = None
        self._playback:        Optional[AudioPlayback]    = None

    # ── public API ────────────────────────────────────────────────────────────
    def start_streaming(self):
        if self._running:
            return
        self._running = True
        mode = self.settings.get("mode", "client")
        if mode == "server":
            self._start_server()
        else:
            self._start_client()

    def stop_streaming(self):
        self._running = False
        self._teardown()
        self.status_changed.emit("stopped")

    def update_settings(self, settings: dict):
        self.settings = settings
        if self._running:
            self.stop_streaming()
            self.start_streaming()

    # ── server ────────────────────────────────────────────────────────────────
    def _start_server(self):
        # Discovery responder
        self._discovery_srv = DiscoveryServer()
        self._discovery_srv.start()

        # Virtual gamepad
        self._vpad = VirtualGamepad()
        self.status_changed.emit("Starting virtual gamepad…")
        if not self._vpad.start():
            err = getattr(self._vpad, "_last_error", "unknown error")
            if "reboot" in err.lower():
                self.status_changed.emit("ViGEm installed — please reboot and try again")
            else:
                self.status_changed.emit(f"ViGEm error: {err}")
            return

        # Wire rumble: ViGEm → TCP → client (connected after receiver starts)
        # We assign after receiver is created below

        # Audio playback
        self._playback = AudioPlayback(self.settings.get("output_device", ""))
        self._playback.start()

        # Stream receiver
        self._receiver = StreamReceiver()
        self._receiver.gamepad_received.connect(self._on_gamepad)
        self._receiver.audio_callback = self._on_audio
        self._receiver.client_connected.connect(
            lambda ip: (self.client_changed.emit(ip),
                        self.status_changed.emit(f"Client: {ip}")))
        self._receiver.client_disconnected.connect(
            lambda: (self.client_changed.emit(""),
                     self.status_changed.emit("Waiting for client…")))
        self._receiver.latency_update.connect(self.latency_update)
        self._receiver.start()

        # Wire rumble: ViGEm callback → TCP → client
        self._vpad.rumble_callback = self._receiver.send_rumble

        self.status_changed.emit("Waiting for client…")

    def _on_gamepad(self, state: dict):
        if self._vpad:
            self._vpad.apply(state)
        self.gamepad_activity.emit()

    def _on_audio(self, pcm: bytes, sr: int, ch: int):
        if self._playback:
            self._playback.push(pcm, sr, ch)

    # ── client ────────────────────────────────────────────────────────────────
    def _start_client(self):
        server_ip = self.settings.get("server_ip", "")
        if not server_ip:
            self.status_changed.emit("Discovering server…")
            self._discover_then_connect()
            return
        self._connect_to(server_ip)

    def _discover_then_connect(self):
        self._disc = DiscoveryClient()
        self._disc.found.connect(self._on_discovered)
        self._disc.start()
        self.status_changed.emit("Scanning network…")

    def _on_discovered(self, servers: list):
        if not servers:
            self.status_changed.emit("No server found – retrying in 5 s")
            QTimer.singleShot(5000, self._discover_then_connect)
            return
        ip = servers[0]
        self.settings["server_ip"] = ip
        save_settings(self.settings)
        self._connect_to(ip)

    def _connect_to(self, ip: str):
        # Gamepad capture
        self._gamepad_cap = GamepadCapture(self.settings.get("gamepad_index", -1))
        self._sender = StreamSender(ip)
        self._gamepad_cap.state_changed.connect(
            lambda s: (self._sender.push_gamepad(s),
                       self.gamepad_activity.emit()))
        self._sender.status_changed.connect(self.status_changed)
        self._sender.latency_update.connect(self.latency_update)

        # Audio capture — direct callback, bypasses Qt event loop
        self._audio_cap = AudioCapture(self.settings.get("capture_device", ""))
        if self.settings.get("send_audio", True):
            self._audio_cap.set_callback(self._sender.push_audio)
        # else: callback stays None → capture thread runs but sends nothing

        self._gamepad_cap.start()
        self._audio_cap.start()
        self._sender.start()

        self.status_changed.emit(f"Connecting to {ip}…")

    # ── teardown ─────────────────────────────────────────────────────────────
    def _teardown(self):
        threads = [
            self._discovery_srv, self._disc, self._gamepad_cap, self._audio_cap,
            self._sender, self._receiver,
        ]

        # Step 1: signal all threads to stop simultaneously
        for t in threads:
            if t is not None:
                try:
                    if t.isRunning():
                        t.requestInterruption()
                except RuntimeError:
                    pass

        # Step 2: wait for each to finish — they have short sleep loops so this is fast
        for t in threads:
            if t is not None:
                try:
                    if t.isRunning():
                        finished = t.wait(3000)   # 3 s max
                        if not finished:
                            log.warning("Thread %s did not stop in time, terminating", t)
                            t.terminate()
                            t.wait(1000)
                except RuntimeError:
                    pass

        # Step 3: stop non-thread subsystems
        if self._playback:
            try:
                self._playback.stop()
            except Exception:
                pass
        if self._vpad:
            try:
                self._vpad.stop()
            except Exception:
                pass

        # Step 4: clear refs — now safe because threads have exited
        self._discovery_srv = None
        self._disc          = None
        self._gamepad_cap   = None
        self._audio_cap     = None
        self._sender        = None
        self._receiver      = None
        self._playback      = None
        self._vpad          = None


# ═════════════════════════════════════════════════════════════════════════════
# UI  ─  dark cyberpunk aesthetic
# ═════════════════════════════════════════════════════════════════════════════

STYLESHEET = """
QWidget {
    background: transparent;
    color: #e8e8e8;
    font-family: 'Segoe UI', 'Arial', sans-serif;
}

QMainWindow, #root {
    background: qlineargradient(
        x1:0, y1:0, x2:1, y2:1,
        stop:0 #0d0d12,
        stop:0.5 #10101a,
        stop:1 #0a0a10
    );
}

/* ── Cards ── */
#card {
    background: rgba(255,255,255,0.03);
    border: 1px solid rgba(14,165,233,0.15);
    border-radius: 12px;
    padding: 16px;
}

#card_active {
    background: rgba(14,165,233,0.07);
    border: 1px solid rgba(14,165,233,0.45);
    border-radius: 12px;
    padding: 16px;
}

/* ── Labels ── */
#title {
    font-size: 22px;
    font-weight: 700;
    color: #ffffff;
    letter-spacing: 2px;
}

#subtitle {
    font-size: 11px;
    color: rgba(14,165,233,0.8);
    letter-spacing: 4px;
    text-transform: uppercase;
}

#label_key {
    font-size: 11px;
    color: rgba(255,255,255,0.4);
    letter-spacing: 1px;
}

#label_val {
    font-size: 13px;
    color: #ffffff;
    font-weight: 600;
}

#status_good  { color: #39ff14; font-size: 12px; font-weight: 600; }
#status_warn  { color: #ffcc00; font-size: 12px; font-weight: 600; }
#status_bad   { color: #ff4060; font-size: 12px; font-weight: 600; }

/* ── Buttons ── */
QPushButton {
    border: none;
    border-radius: 8px;
    padding: 10px 20px;
    font-size: 12px;
    font-weight: 700;
    letter-spacing: 1px;
}

#btn_primary {
    background: qlineargradient(x1:0, y1:0, x2:1, y2:0,
        stop:0 #0ea5e9, stop:1 #0369a1);
    color: white;
}
#btn_primary:hover  { background: qlineargradient(x1:0,y1:0,x2:1,y2:0,stop:0 #38bdf8,stop:1 #0284c7); }
#btn_primary:pressed{ background: qlineargradient(x1:0,y1:0,x2:1,y2:0,stop:0 #0284c7,stop:1 #015d9a); }

#btn_danger {
    background: rgba(255,60,80,0.15);
    border: 1px solid rgba(255,60,80,0.4);
    color: #ff4060;
}
#btn_danger:hover  { background: rgba(255,60,80,0.25); }

#btn_secondary {
    background: rgba(255,255,255,0.05);
    border: 1px solid rgba(255,255,255,0.1);
    color: rgba(255,255,255,0.7);
}
#btn_secondary:hover { background: rgba(255,255,255,0.09); }

/* ── Mode selector tabs ── */
#mode_btn {
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(255,255,255,0.08);
    border-radius: 8px;
    color: rgba(255,255,255,0.4);
    font-size: 13px;
    font-weight: 600;
    padding: 12px 28px;
}
#mode_btn_active {
    background: qlineargradient(x1:0,y1:0,x2:1,y2:0,stop:0 rgba(14,165,233,0.25),stop:1 rgba(3,105,161,0.25));
    border: 1px solid rgba(14,165,233,0.6);
    border-radius: 8px;
    color: #ffffff;
    font-size: 13px;
    font-weight: 700;
    padding: 12px 28px;
}

/* ── Combo boxes ── */
QComboBox {
    background: rgba(255,255,255,0.05);
    border: 1px solid rgba(255,255,255,0.12);
    border-radius: 7px;
    padding: 7px 12px;
    color: #e8e8e8;
    font-size: 12px;
}
QComboBox::drop-down { border: none; width: 24px; }
QComboBox QAbstractItemView {
    background: #1a1a2a;
    border: 1px solid rgba(14,165,233,0.3);
    color: #e8e8e8;
    selection-background-color: rgba(14,165,233,0.3);
}

/* ── Separator ── */
QFrame[frameShape="4"], QFrame[frameShape="5"] {
    color: rgba(255,255,255,0.07);
}

/* ── Scrollbar ── */
QScrollBar:vertical {
    background: transparent; width: 4px;
}
QScrollBar::handle:vertical {
    background: rgba(14,165,233,0.4); border-radius: 2px;
}
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }
"""

# ─── Animated pulse indicator ────────────────────────────────────────────────

class PulseIndicator(QWidget):
    def __init__(self, color="#0ea5e9", size=12, parent=None):
        super().__init__(parent)
        self.setFixedSize(size, size)
        self._color = QColor(color)
        self._alpha = 255
        self._active = False
        self._timer = QTimer(self)
        self._timer.timeout.connect(self._fade)
        self._fade_val = 0

    def pulse(self):
        self._fade_val = 255
        self._active = True
        if not self._timer.isActive():
            self._timer.start(16)
        self.update()

    def set_steady(self, on: bool, color: str = None):
        self._timer.stop()
        self._active = on
        if color:
            self._color = QColor(color)
        self._fade_val = 255 if on else 0
        self.update()

    def _fade(self):
        self._fade_val = max(0, self._fade_val - 18)
        if self._fade_val == 0:
            self._timer.stop()
            self._active = False
        self.update()

    def paintEvent(self, _):
        p = QPainter(self)
        p.setRenderHint(QPainter.RenderHint.Antialiasing)
        c = QColor(self._color)
        c.setAlpha(self._fade_val)
        # Glow
        glow = QColor(c)
        glow.setAlpha(int(self._fade_val * 0.3))
        p.setPen(Qt.PenStyle.NoPen)
        p.setBrush(QBrush(glow))
        p.drawEllipse(0, 0, self.width(), self.height())
        # Core
        margin = 3
        c.setAlpha(self._fade_val)
        p.setBrush(QBrush(c))
        p.drawEllipse(margin, margin,
                      self.width()-margin*2, self.height()-margin*2)


# ─── Main window ─────────────────────────────────────────────────────────────

class MainWindow(QMainWindow):
    def __init__(self, orchestrator: Orchestrator, settings: dict):
        super().__init__()
        self.orch = orchestrator
        self.settings = settings
        self._streaming = False
        self._scan_disc: Optional[DiscoveryClient] = None

        self.setWindowTitle("padStream")
        self.setMinimumSize(700, 340)
        self.setMaximumHeight(480)
        self.setObjectName("root")
        self.setStyleSheet(STYLESHEET)

        self._build_ui()
        self._apply_settings_to_ui()
        self._connect_signals()

        # Auto-connect AFTER event loop starts (singleShot at 0ms runs in first idle tick)
        if settings.get("auto_connect"):
            QTimer.singleShot(1200, self._toggle_stream)

    # ── build UI ──────────────────────────────────────────────────────────────
    def _build_ui(self):
        central = QWidget()
        central.setObjectName("root")
        self.setCentralWidget(central)

        # ── Two-column root layout ───────────────────────────────────────────
        root_layout = QHBoxLayout(central)
        root_layout.setContentsMargins(16, 16, 16, 16)
        root_layout.setSpacing(12)

        # ════ LEFT COLUMN — header, mode, status, buttons ════════════════════
        left = QVBoxLayout()
        left.setSpacing(10)
        left.setContentsMargins(0, 0, 0, 0)

        # Header
        lbl_title = QLabel("PADSTREAM")
        lbl_title.setObjectName("title")
        lbl_sub = QLabel("GAMEPAD & AUDIO BRIDGE")
        lbl_sub.setObjectName("subtitle")
        left.addWidget(lbl_title)
        left.addWidget(lbl_sub)

        # Indicators row
        ind_row = QHBoxLayout(); ind_row.setSpacing(10)
        self._ind_gp    = PulseIndicator("#0ea5e9", 10)
        self._ind_audio = PulseIndicator("#39ff14", 10)
        gi = QHBoxLayout(); gi.setSpacing(4)
        gi.addWidget(QLabel("GP")); gi.addWidget(self._ind_gp)
        ai = QHBoxLayout(); ai.setSpacing(4)
        ai.addWidget(QLabel("AUD")); ai.addWidget(self._ind_audio)
        for lbl in [gi.itemAt(0).widget(), ai.itemAt(0).widget()]:
            lbl.setObjectName("label_key")
        ind_row.addLayout(gi); ind_row.addLayout(ai); ind_row.addStretch()
        left.addLayout(ind_row)

        # Mode selector
        mode_card = QWidget(); mode_card.setObjectName("card")
        ml = QVBoxLayout(mode_card); ml.setContentsMargins(10, 10, 10, 10); ml.setSpacing(6)
        mode_lbl = QLabel("MODE"); mode_lbl.setObjectName("label_key")
        ml.addWidget(mode_lbl)
        mode_row = QHBoxLayout(); mode_row.setSpacing(6)
        self._btn_client = QPushButton("◀  CLIENT")
        self._btn_server = QPushButton("SERVER  ▶")
        self._btn_client.setObjectName("mode_btn_active")
        self._btn_server.setObjectName("mode_btn")
        self._btn_client.clicked.connect(lambda: self._set_mode("client"))
        self._btn_server.clicked.connect(lambda: self._set_mode("server"))
        mode_row.addWidget(self._btn_client)
        mode_row.addWidget(self._btn_server)
        ml.addLayout(mode_row)
        left.addWidget(mode_card)

        # Status card
        stat_card = QWidget(); stat_card.setObjectName("card")
        sl = QVBoxLayout(stat_card); sl.setContentsMargins(10, 10, 10, 10); sl.setSpacing(4)
        stat_hdr = QHBoxLayout()
        lbl_stat_key = QLabel("STATUS"); lbl_stat_key.setObjectName("label_key")
        self._lbl_latency = QLabel(""); self._lbl_latency.setObjectName("label_key")
        stat_hdr.addWidget(lbl_stat_key); stat_hdr.addStretch(); stat_hdr.addWidget(self._lbl_latency)
        self._lbl_status = QLabel("Idle"); self._lbl_status.setObjectName("status_warn")
        sl.addLayout(stat_hdr); sl.addWidget(self._lbl_status)
        left.addWidget(stat_card)

        left.addStretch()

        # Start/Stop button
        self._btn_action = QPushButton("▶  START STREAMING")
        self._btn_action.setObjectName("btn_primary")
        self._btn_action.setMinimumHeight(40)
        self._btn_action.clicked.connect(self._toggle_stream)
        left.addWidget(self._btn_action)

        # Scan / refresh
        bottom_row = QHBoxLayout(); bottom_row.setSpacing(6)
        self._btn_scan = QPushButton("⟳  Scan")
        self._btn_scan.setObjectName("btn_secondary")
        self._btn_scan.clicked.connect(self._scan_network)
        self._btn_refresh = QPushButton("↺  Refresh")
        self._btn_refresh.setObjectName("btn_secondary")
        self._btn_refresh.clicked.connect(self._refresh_devices)
        bottom_row.addWidget(self._btn_scan)
        bottom_row.addWidget(self._btn_refresh)
        left.addLayout(bottom_row)

        root_layout.addLayout(left, stretch=0)

        # ── Vertical divider ────────────────────────────────────────────────
        div = QFrame()
        div.setFrameShape(QFrame.Shape.VLine)
        div.setFrameShadow(QFrame.Shadow.Sunken)
        root_layout.addWidget(div)

        # ════ RIGHT COLUMN — settings stack ══════════════════════════════════
        self._stack = QStackedWidget()
        self._stack.addWidget(self._build_client_settings())   # 0
        self._stack.addWidget(self._build_server_settings())   # 1
        root_layout.addWidget(self._stack, stretch=1)

    def _build_client_settings(self) -> QWidget:
        w = QWidget()
        lay = QVBoxLayout(w); lay.setContentsMargins(0, 0, 0, 0); lay.setSpacing(10)

        # Gamepad
        gp_card = QWidget(); gp_card.setObjectName("card")
        gl = QVBoxLayout(gp_card); gl.setContentsMargins(12, 12, 12, 12); gl.setSpacing(6)
        lbl = QLabel("GAMEPAD"); lbl.setObjectName("label_key")
        self._combo_gamepad = QComboBox()
        self._combo_gamepad.addItem("🎮  Auto (first active)")
        for idx, name in GamepadCapture.list_gamepads():
            self._combo_gamepad.addItem(f"[{idx}] {name}", idx)
        gl.addWidget(lbl)
        gl.addWidget(self._combo_gamepad)
        lay.addWidget(gp_card)

        # Capture device
        cap_card = QWidget(); cap_card.setObjectName("card")
        cl = QVBoxLayout(cap_card); cl.setContentsMargins(12, 12, 12, 12); cl.setSpacing(8)
        # Header row: label + toggle
        cap_hdr = QHBoxLayout()
        lbl2 = QLabel("CAPTURE DEVICE  (loopback)"); lbl2.setObjectName("label_key")
        self._btn_send_audio = QPushButton("🔇  Mute")
        self._btn_send_audio.setObjectName("btn_secondary")
        self._btn_send_audio.setFixedWidth(90)
        self._btn_send_audio.setCheckable(True)
        self._btn_send_audio.setChecked(True)
        self._btn_send_audio.clicked.connect(self._toggle_send_audio)
        cap_hdr.addWidget(lbl2)
        cap_hdr.addStretch()
        cap_hdr.addWidget(self._btn_send_audio)
        self._combo_capture = QComboBox()
        self._combo_capture.addItem("🔊  Default Speakers (loopback)")
        for idx, name in AudioCapture.list_loopback_devices():
            self._combo_capture.addItem(name, name)
        cl.addLayout(cap_hdr)
        cl.addWidget(self._combo_capture)
        lay.addWidget(cap_card)

        # Server IP
        srv_card = QWidget(); srv_card.setObjectName("card")
        svl = QVBoxLayout(srv_card); svl.setContentsMargins(12, 12, 12, 12); svl.setSpacing(6)
        lbl3 = QLabel("SERVER"); lbl3.setObjectName("label_key")
        self._combo_server = QComboBox()
        self._combo_server.setEditable(True)
        self._combo_server.addItem("Auto-discover")
        svl.addWidget(lbl3)
        svl.addWidget(self._combo_server)
        lay.addWidget(srv_card)

        return w

    def _build_server_settings(self) -> QWidget:
        w = QWidget()
        lay = QVBoxLayout(w); lay.setContentsMargins(0, 0, 0, 0); lay.setSpacing(10)

        out_card = QWidget(); out_card.setObjectName("card")
        ol = QVBoxLayout(out_card); ol.setContentsMargins(12, 12, 12, 12); ol.setSpacing(6)
        lbl = QLabel("OUTPUT DEVICE"); lbl.setObjectName("label_key")
        self._combo_output = QComboBox()
        self._combo_output.addItem("🔊  Default Output")
        for idx, name in AudioPlayback.list_output_devices():
            self._combo_output.addItem(name, name)
        ol.addWidget(lbl)
        ol.addWidget(self._combo_output)
        lay.addWidget(out_card)

        info_card = QWidget(); info_card.setObjectName("card")
        il = QVBoxLayout(info_card); il.setContentsMargins(12, 12, 12, 12); il.setSpacing(8)
        lbl2 = QLabel("SERVER INFO"); lbl2.setObjectName("label_key")
        self._lbl_my_ip = QLabel(self._get_local_ip())
        self._lbl_my_ip.setObjectName("label_val")
        self._lbl_client_ip = QLabel("No client connected")
        self._lbl_client_ip.setObjectName("label_key")
        row = QHBoxLayout()
        row.addWidget(QLabel("Local IP:"))
        row.addWidget(self._lbl_my_ip)
        row.addStretch()
        il.addWidget(lbl2)
        il.addLayout(row)
        il.addWidget(self._lbl_client_ip)
        lay.addWidget(info_card)
        lay.addStretch()
        return w

    # ── signals ───────────────────────────────────────────────────────────────
    def _connect_signals(self):
        self.orch.status_changed.connect(self._on_status)
        self.orch.latency_update.connect(self._on_latency)
        self.orch.client_changed.connect(self._on_client_changed)
        self.orch.gamepad_activity.connect(self._ind_gp.pulse)

    # ── slots ─────────────────────────────────────────────────────────────────
    def _on_status(self, msg: str):
        self._lbl_status.setText(msg)
        if "connect" in msg.lower() or "client" in msg.lower():
            self._lbl_status.setObjectName("status_good")
            self._ind_audio.set_steady(True, "#39ff14")
        elif "error" in msg.lower() or "no server" in msg.lower():
            self._lbl_status.setObjectName("status_bad")
        else:
            self._lbl_status.setObjectName("status_warn")
        self._lbl_status.setStyleSheet("")   # force refresh

    def _on_latency(self, ms: float):
        self._lbl_latency.setText(f"⚡ {ms:.1f} ms")

    def _on_client_changed(self, ip: str):
        if hasattr(self, "_lbl_client_ip"):
            self._lbl_client_ip.setText(f"Client: {ip}" if ip else "No client connected")

    # ── actions ───────────────────────────────────────────────────────────────
    def _set_mode(self, mode: str):
        self.settings["mode"] = mode
        self._stack.setCurrentIndex(0 if mode == "client" else 1)
        self._btn_client.setObjectName("mode_btn_active" if mode == "client" else "mode_btn")
        self._btn_server.setObjectName("mode_btn_active" if mode == "server" else "mode_btn")
        self._btn_client.setStyleSheet("")
        self._btn_server.setStyleSheet("")
        save_settings(self.settings)

    def _toggle_stream(self):
        if not self._streaming:
            self._save_ui_to_settings()
            self.orch.start_streaming()
            self._streaming = True
            self._btn_action.setText("⏹  STOP STREAMING")
            self._btn_action.setObjectName("btn_danger")
            self._btn_action.setStyleSheet("")
        else:
            self.orch.stop_streaming()
            self._streaming = False
            self._btn_action.setText("▶  START STREAMING")
            self._btn_action.setObjectName("btn_primary")
            self._btn_action.setStyleSheet("")
            self._lbl_status.setText("Stopped")
            self._lbl_status.setObjectName("status_warn")
            self._lbl_status.setStyleSheet("")
            self._lbl_latency.setText("")
            self._ind_audio.set_steady(False)
            self._ind_gp.set_steady(False)

    def _toggle_send_audio(self):
        sending = self._btn_send_audio.isChecked()
        self._btn_send_audio.setText("🔇  Mute" if sending else "🔊  Sending")
        self._combo_capture.setEnabled(sending)
        self.settings["send_audio"] = sending
        save_settings(self.settings)
        # Apply live if streaming
        if self._streaming and self.orch._audio_cap:
            if sending:
                self.orch._audio_cap.set_callback(self.orch._sender.push_audio)
            else:
                self.orch._audio_cap.set_callback(None)

    def _scan_network(self):
        self._lbl_status.setText("Scanning…")
        self._scan_disc = DiscoveryClient()
        self._scan_disc.found.connect(self._on_scan_done)
        self._scan_disc.start()

    def _on_scan_done(self, servers: list):
        if not hasattr(self, "_combo_server"):
            return
        self._combo_server.clear()
        self._combo_server.addItem("Auto-discover")
        for ip in servers:
            self._combo_server.addItem(ip)
        self._lbl_status.setText(f"Found {len(servers)} server(s)")

    def _refresh_devices(self):
        # Gamepad
        if hasattr(self, "_combo_gamepad"):
            self._combo_gamepad.clear()
            self._combo_gamepad.addItem("🎮  Auto (first active)")
            for idx, name in GamepadCapture.list_gamepads():
                self._combo_gamepad.addItem(f"[{idx}] {name}", idx)

        # Capture
        if hasattr(self, "_combo_capture"):
            self._combo_capture.clear()
            self._combo_capture.addItem("🔊  Default Speakers (loopback)")
            for idx, name in AudioCapture.list_loopback_devices():
                self._combo_capture.addItem(name, name)

        # Output
        if hasattr(self, "_combo_output"):
            self._combo_output.clear()
            self._combo_output.addItem("🔊  Default Output")
            for idx, name in AudioPlayback.list_output_devices():
                self._combo_output.addItem(name, name)

    # ── settings helpers ──────────────────────────────────────────────────────
    def _apply_settings_to_ui(self):
        mode = self.settings.get("mode", "client")
        self._set_mode(mode)

        # Send audio toggle
        if hasattr(self, "_btn_send_audio"):
            sending = self.settings.get("send_audio", True)
            self._btn_send_audio.setChecked(sending)
            self._btn_send_audio.setText("🔇  Mute" if sending else "🔊  Sending")
            if hasattr(self, "_combo_capture"):
                self._combo_capture.setEnabled(sending)

        # Server IP
        saved_ip = self.settings.get("server_ip", "")
        if saved_ip and hasattr(self, "_combo_server"):
            idx = self._combo_server.findText(saved_ip)
            if idx < 0:
                self._combo_server.addItem(saved_ip)
                idx = self._combo_server.findText(saved_ip)
            self._combo_server.setCurrentIndex(idx)

    def _save_ui_to_settings(self):
        if hasattr(self, "_combo_server"):
            txt = self._combo_server.currentText()
            self.settings["server_ip"] = "" if txt == "Auto-discover" else txt

        if hasattr(self, "_combo_gamepad"):
            data = self._combo_gamepad.currentData()
            self.settings["gamepad_index"] = data if data is not None else -1

        if hasattr(self, "_combo_capture"):
            data = self._combo_capture.currentData()
            self.settings["capture_device"] = data if data is not None else ""

        if hasattr(self, "_btn_send_audio"):
            self.settings["send_audio"] = self._btn_send_audio.isChecked()

        if hasattr(self, "_combo_output"):
            data = self._combo_output.currentData()
            self.settings["output_device"] = data if data is not None else ""

        save_settings(self.settings)
        self.orch.settings = self.settings

    @staticmethod
    def _get_local_ip() -> str:
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            return ip
        except Exception:
            return "Unknown"

    # ── window events ─────────────────────────────────────────────────────────
    def closeEvent(self, event):
        # X button → quit completely
        self.orch.stop_streaming()
        event.accept()
        QApplication.quit()

    def changeEvent(self, event):
        from PyQt6.QtCore import QEvent
        if event.type() == QEvent.Type.WindowStateChange:
            if self.isMinimized():
                event.ignore()
                self.hide()   # minimize → tray
                return
        super().changeEvent(event)


# ═════════════════════════════════════════════════════════════════════════════
# TRAY ICON
# ═════════════════════════════════════════════════════════════════════════════

def _make_tray_icon() -> QIcon:
    """Generate a small blue gamepad icon."""
    px = QPixmap(64, 64)
    px.fill(Qt.GlobalColor.transparent)
    p = QPainter(px)
    p.setRenderHint(QPainter.RenderHint.Antialiasing)

    # Body
    grad = QLinearGradient(0, 0, 64, 64)
    grad.setColorAt(0, QColor("#0ea5e9"))
    grad.setColorAt(1, QColor("#0369a1"))
    p.setBrush(QBrush(grad))
    p.setPen(Qt.PenStyle.NoPen)
    p.drawRoundedRect(8, 20, 48, 28, 12, 12)

    # Left grip
    p.drawRoundedRect(4, 32, 18, 22, 8, 8)
    # Right grip
    p.drawRoundedRect(42, 32, 18, 22, 8, 8)

    # D-pad hint
    p.setBrush(QBrush(QColor("white")))
    p.setOpacity(0.6)
    p.drawRect(16, 28, 4, 12)
    p.drawRect(12, 32, 12, 4)

    # Buttons
    for cx, cy, col in [(42, 26, "#ff4060"), (47, 30, "#39ff14"),
                        (37, 30, "#ffcc00"), (42, 34, "#4af")]:
        p.setOpacity(1.0)
        p.setBrush(QBrush(QColor(col)))
        p.drawEllipse(cx-3, cy-3, 6, 6)

    p.end()
    return QIcon(px)


class TrayApp:
    def __init__(self, app: QApplication, window: MainWindow, orch: Orchestrator):
        self.app    = app
        self.window = window
        self.orch   = orch

        self.tray = QSystemTrayIcon(app)
        self.tray.setIcon(_make_tray_icon())
        self.tray.setToolTip("padStream")

        menu = QMenu()
        act_show  = QAction("Open padStream", app)
        act_start = QAction("▶  Start Streaming", app)
        act_stop  = QAction("⏹  Stop Streaming", app)
        act_quit  = QAction("Quit", app)

        act_show.triggered.connect(self._show_window)
        act_start.triggered.connect(window._toggle_stream)
        act_stop.triggered.connect(window._toggle_stream)
        act_quit.triggered.connect(self._quit)

        menu.addAction(act_show)
        menu.addSeparator()
        menu.addAction(act_start)
        menu.addAction(act_stop)
        menu.addSeparator()
        menu.addAction(act_quit)

        self.tray.setContextMenu(menu)
        self.tray.activated.connect(self._on_tray_click)
        self.tray.show()

        # Update tooltip with status
        orch.status_changed.connect(lambda s: self.tray.setToolTip(f"padStream – {s}"))

    def _show_window(self):
        self.window.show()
        self.window.raise_()
        self.window.activateWindow()

    def _on_tray_click(self, reason):
        if reason == QSystemTrayIcon.ActivationReason.DoubleClick:
            self._show_window()

    def _quit(self):
        self.window.close()   # triggers closeEvent → stop + quit


# ═════════════════════════════════════════════════════════════════════════════
# ENTRY POINT
# ═════════════════════════════════════════════════════════════════════════════

def main():
    # Let Qt handle DPI — avoid SetProcessDpiAwareness which needs elevation
    os.environ.setdefault("QT_ENABLE_HIGHDPI_SCALING", "1")

    app = QApplication(sys.argv)
    app.setQuitOnLastWindowClosed(False)
    app.setStyle("Fusion")

    settings     = load_settings()
    orchestrator = Orchestrator(settings)
    window       = MainWindow(orchestrator, settings)
    tray         = TrayApp(app, window, orchestrator)

    # Guarantee cleanup before Qt destroys C++ objects
    app.aboutToQuit.connect(orchestrator.stop_streaming)

    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()