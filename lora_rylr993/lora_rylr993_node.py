import json
import argparse
import random
import sys
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import rclpy
from rclpy.logging import LoggingSeverity
from rclpy.node import Node
from std_msgs.msg import String

try:
    import serial
except ImportError:  # pragma: no cover - handled at runtime on target
    serial = None


def parse_bw_to_code(bw: str) -> int:
    s = str(bw).lower().strip().replace("khz", "k").replace(" ", "")
    if s in ("7", "8", "9"):
        return int(s)
    if s.endswith("k"):
        s = s[:-1]
    if s in ("125", "125000"):
        return 7
    if s in ("250", "250000"):
        return 8
    if s in ("500", "500000"):
        return 9
    raise ValueError(f"Unsupported bw: {bw}")


def resolve_port(port_arg) -> str:
    s = str(port_arg).strip()
    if s.isdigit():
        return f"/dev/ttyUSB{s}"
    return s


def parse_robots(expr: str) -> List[int]:
    parts = expr.replace(" ", ",").split(",")
    out = set()
    for p in parts:
        if not p:
            continue
        if "-" in p:
            a, b = p.split("-")
            for x in range(int(a), int(b) + 1):
                out.add(x)
        else:
            out.add(int(p))
    return sorted(out)


def sleep_until(deadline_mono: float, busy_tail_s: float = 0.002):
    while True:
        remain = deadline_mono - time.monotonic()
        if remain <= 0:
            return
        if remain > busy_tail_s:
            time.sleep(remain - busy_tail_s)


def write_cmd(ser, cmd: str):
    ser.write((cmd + "\r\n").encode())


def _readline_text(ser, raw_lines: Optional[list[str]] = None) -> str:
    raw = ser.readline()
    if raw and raw_lines is not None:
        raw_lines.append(repr(raw))
    return raw.decode(errors="ignore").strip()


def at_expect_ok(
    ser, cmd: str, wait_s: float, raw_lines: Optional[list[str]] = None
) -> Tuple[bool, list[str]]:
    write_cmd(ser, cmd)
    end = time.time() + wait_s
    lines: list[str] = []
    while time.time() < end:
        line = _readline_text(ser, raw_lines=raw_lines)
        if line:
            lines.append(line)
        else:
            time.sleep(0.002)
        if line == "OK":
            return True, lines
    return False, lines


def at_collect(ser, cmd: str, wait_s: float, raw_lines: Optional[list[str]] = None) -> list[str]:
    write_cmd(ser, cmd)
    end = time.time() + wait_s
    lines: list[str] = []
    while time.time() < end:
        line = _readline_text(ser, raw_lines=raw_lines)
        if line:
            lines.append(line)
        else:
            time.sleep(0.002)
    return lines


def wait_ready(ser, timeout_s: float, raw_lines: Optional[list[str]] = None) -> Tuple[bool, list[str]]:
    end = time.time() + timeout_s
    lines: list[str] = []
    while time.time() < end:
        line = _readline_text(ser, raw_lines=raw_lines)
        if line:
            lines.append(line)
        else:
            time.sleep(0.002)
        if "+READY" in line:
            return True, lines
    return False, lines


def drain_uart(ser, seconds: float, raw_lines: Optional[list[str]] = None):
    end = time.time() + seconds
    while time.time() < end:
        line = _readline_text(ser, raw_lines=raw_lines)
        if not line:
            time.sleep(0.002)


def _fmt_lines(lines: list[str], max_items: int = 8) -> str:
    if not lines:
        return "[]"
    if len(lines) <= max_items:
        return "[" + " | ".join(lines) + "]"
    shown = " | ".join(lines[:max_items])
    return f"[{shown} | ... +{len(lines) - max_items} more]"


def _fmt_raw_lines(lines: list[str], max_items: int = 12) -> str:
    if not lines:
        return "[]"
    if len(lines) <= max_items:
        return "[" + " | ".join(lines) + "]"
    shown = " | ".join(lines[:max_items])
    return f"[{shown} | ... +{len(lines) - max_items} more]"


def init_radio(ser, cfg: "LoraConfig", debug_raw_uart: bool = False) -> Tuple[bool, str]:
    # Robust modem handshake/config flow aligned with lora_tdma.py.
    debug: list[str] = []
    raw_lines: Optional[list[str]] = [] if debug_raw_uart else None

    ok, lines = at_expect_ok(ser, "AT", 0.5, raw_lines=raw_lines)
    debug.append(f"AT: ok={ok} lines={_fmt_lines(lines)}")
    if not ok:
        write_cmd(ser, "ATZ")
        ready_ok, ready_lines = wait_ready(ser, 4.0, raw_lines=raw_lines)
        debug.append(f"ATZ->READY: ok={ready_ok} lines={_fmt_lines(ready_lines)}")
        if not ready_ok:
            return False, " ; ".join(debug)
        ok, lines = at_expect_ok(ser, "AT", 1.0, raw_lines=raw_lines)
        debug.append(f"AT(retry): ok={ok} lines={_fmt_lines(lines)}")
        if not ok:
            return False, " ; ".join(debug)

    write_cmd(ser, "AT+OPMODE=1")
    time.sleep(0.6)
    need_reset = False
    while True:
        line = _readline_text(ser, raw_lines=raw_lines)
        if not line:
            break
        if "Need RESET" in line:
            need_reset = True

    debug.append(f"AT+OPMODE=1: need_reset={need_reset}")
    if need_reset:
        write_cmd(ser, "ATZ")
        ready_ok, ready_lines = wait_ready(ser, 4.0, raw_lines=raw_lines)
        debug.append(f"ATZ(after Need RESET)->READY: ok={ready_ok} lines={_fmt_lines(ready_lines)}")
        if not ready_ok:
            return False, " ; ".join(debug)

    bw_code = parse_bw_to_code(cfg.bw)
    lines = at_collect(ser, f"AT+ADDRESS={cfg.address}", 0.6, raw_lines=raw_lines)
    debug.append(f"AT+ADDRESS={cfg.address}: lines={_fmt_lines(lines)}")
    lines = at_collect(ser, f"AT+BAND={cfg.band}", 0.6, raw_lines=raw_lines)
    debug.append(f"AT+BAND={cfg.band}: lines={_fmt_lines(lines)}")
    lines = at_collect(
        ser, f"AT+PARAMETER={cfg.sf},{bw_code},{cfg.cr_code},{cfg.preamble}", 0.8, raw_lines=raw_lines
    )
    debug.append(f"AT+PARAMETER={cfg.sf},{bw_code},{cfg.cr_code},{cfg.preamble}: lines={_fmt_lines(lines)}")
    lines = at_collect(ser, f"AT+CRFOP={cfg.tx_power}", 0.6, raw_lines=raw_lines)
    debug.append(f"AT+CRFOP={cfg.tx_power}: lines={_fmt_lines(lines)}")
    lines = at_collect(ser, "AT+PARAMETER=?", 0.8, raw_lines=raw_lines)
    debug.append(f"AT+PARAMETER=?: lines={_fmt_lines(lines)}")
    drain_uart(ser, 0.8, raw_lines=raw_lines)

    verify_cmds = [f"AT+ADDRESS={cfg.address}"]
    for cmd in verify_cmds:
        ok, lines = at_expect_ok(ser, cmd, 0.8, raw_lines=raw_lines)
        debug.append(f"verify {cmd}: ok={ok} lines={_fmt_lines(lines)}")
        if not ok:
            return False, " ; ".join(debug)

    if raw_lines is not None:
        debug.append(f"raw_uart={_fmt_raw_lines(raw_lines)}")
    return True, " ; ".join(debug)


def parse_rcv(line: str) -> Optional[Tuple[int, int, str, int, int]]:
    # +RCV=<src>,<len>,<data>,<rssi>,<snr>
    if not line.startswith("+RCV="):
        return None
    try:
        p = line[5:].split(",")
        if len(p) < 5:
            return None
        src = int(p[0])
        ln = int(p[1])
        data = ",".join(p[2:-2])
        rssi = int(p[-2])
        snr = int(p[-1])
        if ln != len(data):
            return None
        return src, ln, data, rssi, snr
    except (TypeError, ValueError):
        return None


def extract_id_data(payload: str) -> Optional[Tuple[str, str]]:
    try:
        obj = json.loads(payload)
    except json.JSONDecodeError:
        return None

    if not isinstance(obj, dict):
        return None

    id_val = obj.get("id")
    data_val = obj.get("d")
    if id_val is None or data_val is None:
        return None
    return str(id_val), str(data_val)


def parse_master_pos_field(field: str):
    if not field.startswith("P:"):
        return None
    try:
        parts = field.split(":")
        if len(parts) != 7:
            return None
        return {
            "id": int(parts[1]),
            "seq": int(parts[2], 16),
            "x": int(parts[3]),
            "y": int(parts[4]),
            "heading": int(parts[5]),
            "status": int(parts[6]),
        }
    except (TypeError, ValueError):
        return None


def make_master_pos_field(master_id: int, seq: int, x: int, y: int, heading: int, status: int) -> str:
    return f"P:{master_id}:{seq:02X}:{x}:{y}:{heading}:{status}"


def make_beacon(
    frame_id: int,
    uuid: str,
    payload: str,
    offer_uuid: Optional[str] = None,
    offer_id: Optional[int] = None,
) -> str:
    offer_str = ""
    if offer_uuid and offer_id is not None:
        offer_str = f"{offer_uuid}:{offer_id}"
    return f"BCN,{frame_id:04d},{uuid or ''},{offer_str},{payload}"


def parse_beacon(data: str):
    # BCN,<frame>,<uuid>,<offer>,<payload>
    if not data.startswith("BCN,"):
        return None
    try:
        parts = data.split(",")
        if len(parts) < 5:
            return None
        payload = ",".join(parts[4:])
        offer_uuid = None
        offer_id = None
        if parts[3] and ":" in parts[3]:
            offer_parts = parts[3].split(":", 1)
            if len(offer_parts) == 2:
                offer_uuid = offer_parts[0]
                offer_id = int(offer_parts[1])
        return {
            "frame": int(parts[1]),
            "uuid": parts[2],
            "offer_uuid": offer_uuid,
            "offer_id": offer_id,
            "payload": payload,
            "master_pos": parse_master_pos_field(payload),
        }
    except (TypeError, ValueError):
        return None


def make_pos_payload(robot_id: int, seq: int, x: int, y: int, heading: int, status: int) -> str:
    return f"POS,{robot_id},{seq:02X},{x},{y},{heading},{status}"


def parse_pos_payload(data: str):
    if not data.startswith("POS,"):
        return None
    try:
        parts = data.split(",")
        if len(parts) != 7:
            return None
        return {
            "id": int(parts[1]),
            "seq": int(parts[2], 16),
            "x": int(parts[3]),
            "y": int(parts[4]),
            "heading": int(parts[5]),
            "status": int(parts[6]),
        }
    except (TypeError, ValueError):
        return None


def parse_fleet_payload(data: str) -> Optional[Tuple[str, str]]:
    if not isinstance(data, str) or ":" not in data:
        return None
    id_val, payload_val = data.split(":", 1)
    id_val = id_val.strip()
    payload_val = payload_val.strip()
    if not id_val or not payload_val:
        return None
    # Accept compact token-like ids only; avoids matching beacon CSV bodies.
    if not all(c.isalnum() or c in ("-", "_") for c in id_val):
        return None
    # Ignore control/message-type prefixes that are not fleet ids.
    if id_val.lower() in {"join", "bcn", "pos"}:
        return None
    return id_val, payload_val


@dataclass
class LoraConfig:
    port: str = "/dev/ttyUSB0"
    baud: int = 9600
    address: int = 1
    band: int = 915000000
    sf: int = 5
    bw: str = "500"
    cr_code: int = 1
    preamble: int = 12
    tx_power: int = 22


@dataclass
class Stats:
    n: int = 0
    min_v: float = float("inf")
    max_v: float = float("-inf")
    sum_v: float = 0.0

    def add(self, value: float):
        self.n += 1
        self.sum_v += value
        if value < self.min_v:
            self.min_v = value
        if value > self.max_v:
            self.max_v = value

    def avg(self) -> float:
        return self.sum_v / self.n if self.n else 0.0

    def fmt(self) -> str:
        if self.n == 0:
            return "n=0"
        return f"n={self.n} min={self.min_v:.1f}ms avg={self.avg():.1f}ms max={self.max_v:.1f}ms"


class LoraRylr993Node(Node):
    def __init__(self, log_cli: bool = False, log_rx_cli: bool = False, log_calc_cli: bool = False):
        super().__init__("lora_rylr993_node")
        self.publisher_ = self.create_publisher(String, "/fleet_receive", 10)
        self.subscription_ = self.create_subscription(
            String,
            "/fleet_transmit",
            self.fleet_transmit_callback,
            10,
        )

        self._log_cli = bool(log_cli)
        self._log_rx_cli = bool(log_rx_cli)
        self._log_calc_cli = bool(log_calc_cli)
        self.cfg = self._load_config()
        self.log = bool(self.get_parameter("log").value)
        self.log_rx = bool(self.get_parameter("log_rx").value)
        self.log_calc = bool(self.get_parameter("log_calc").value)
        if not self.log:
            # Keep warnings/errors visible while suppressing info/debug logs.
            self.get_logger().set_level(LoggingSeverity.WARN)
        self.debug_raw_uart = bool(self.get_parameter("debug_raw_uart").value)
        self.my_uuid = f"{random.randint(0, 0xFFFF):04X}"
        self.role = "idle"
        self.auto_failover = False
        self.last_frame = -1
        self.last_beacon_mono = time.monotonic()
        self.discovery_deadline_mono = 0.0
        self.discovery_timeout_s = 0.0
        self.auto_id_seen_beacon = False
        self.join_cooldown_frames = 0
        self.frame_id = 0
        self.next_frame_start = time.monotonic()
        self.pending_offer_uuid: Optional[str] = None
        self.pending_offer_id: Optional[int] = None
        self.pending_offer_ttl = 0
        self.peer_table: Dict[int, Dict[str, Any]] = {}
        self.per_expected: Dict[int, int] = {}
        self.per_ok: Dict[int, int] = {}
        self.per_mismatch: Dict[int, int] = {}
        self.robot_order: Dict[int, int] = {}
        self.slot_offset_stats: Dict[int, Stats] = {}
        self.beacon_to_rx_stats: Dict[int, Stats] = {}
        self.last_server_frame_start_mono = time.monotonic()
        self.next_report_mono = time.monotonic() + float(self.get_parameter("report_interval_s").value)
        self.auto_id_registry: Dict[str, int] = {}
        self.last_heard_mono: Dict[int, float] = {}
        self.joined_at_frame: Dict[int, int] = {}
        self.message_fleet_transmit = "0:0"
        self._last_fleet_transmit_rx: Optional[str] = None
        self._last_fleet_receive_key: Optional[str] = None
        self._rx_buffer = ""
        self.ser = None
        self._init_serial()
        self._start_mode()
        self.poll_timer = self.create_timer(0.05, self.poll_lora_callback)

        self.get_logger().info(
            "Monitoring /fleet_transmit, parsing {id,d} for LoRa TX, and publishing /fleet_receive from LoRa RX"
        )

    def _load_config(self) -> LoraConfig:
        self.declare_parameter("log", bool(self._log_cli))
        self.declare_parameter("log_rx", bool(self._log_rx_cli))
        self.declare_parameter("log_calc", bool(self._log_calc_cli))
        self.declare_parameter("port", "/dev/ttyUSB0")
        self.declare_parameter("baud", 9600)
        self.declare_parameter("address", 1)
        self.declare_parameter("band", 915000000)
        self.declare_parameter("sf", 5)
        self.declare_parameter("bw", "500")
        self.declare_parameter("cr_code", 1)
        self.declare_parameter("preamble", 12)
        self.declare_parameter("tx_power", 22)
        self.declare_parameter("debug_raw_uart", False)
        # TDMA runtime params aligned to lora_tdma.py defaults.
        self.declare_parameter("tdma_mode", "auto_id")  # server | client | auto_role | auto_id
        self.declare_parameter("master", 1)
        self.declare_parameter("robots", "1-4")
        self.declare_parameter("base_delay", 0.12)
        self.declare_parameter("slot", 0.06)
        self.declare_parameter("tx_offset", 0.010)
        self.declare_parameter("frame", 0.0)  # <=0 means auto-calc
        self.declare_parameter("margin", 0.020)
        self.declare_parameter("warmup", 10)
        self.declare_parameter("report_interval_s", 10.0)
        self.declare_parameter("peer_timeout_s", 5.0)
        self.declare_parameter("assumed_jitter_ms", 1.8)
        self.declare_parameter("rx_delay_ms", 166.0)
        self.declare_parameter("busy_tail_ms", 2.0)
        self.declare_parameter("late_drop_ms", 30.0)
        self.declare_parameter("listen_timeout", 6.0)
        self.declare_parameter("lease_timeout_s", 30.0)
        self.declare_parameter("x", 0)
        self.declare_parameter("y", 0)
        self.declare_parameter("heading", 0)
        self.declare_parameter("status", 0)
        return LoraConfig(
            port=resolve_port(self.get_parameter("port").value),
            baud=int(self.get_parameter("baud").value),
            address=int(self.get_parameter("address").value),
            band=int(self.get_parameter("band").value),
            sf=int(self.get_parameter("sf").value),
            bw=str(self.get_parameter("bw").value),
            cr_code=int(self.get_parameter("cr_code").value),
            preamble=int(self.get_parameter("preamble").value),
            tx_power=int(self.get_parameter("tx_power").value),
        )

    def _robot_list(self) -> List[int]:
        return parse_robots(str(self.get_parameter("robots").value))

    def _frame_duration(self) -> float:
        frame = float(self.get_parameter("frame").value)
        if frame > 0:
            return frame
        robots = self._robot_list()
        jitter_s = float(self.get_parameter("assumed_jitter_ms").value) / 1000.0
        return (
            float(self.get_parameter("base_delay").value)
            + max(0, len(robots) - 1) * float(self.get_parameter("slot").value)
            + float(self.get_parameter("tx_offset").value)
            + jitter_s
            + float(self.get_parameter("margin").value)
        )

    def _log_auto_frame_if_needed(self, robots: List[int], frame: float):
        if not self.log_calc:
            return
        user_frame = float(self.get_parameter("frame").value)
        if user_frame > 0:
            return
        base_delay = float(self.get_parameter("base_delay").value)
        slot = float(self.get_parameter("slot").value)
        tx_offset = float(self.get_parameter("tx_offset").value)
        jitter_s = float(self.get_parameter("assumed_jitter_ms").value) / 1000.0
        margin = float(self.get_parameter("margin").value)
        self.get_logger().info(
            f"[AUTO-FRAME] Calculated frame duration: {frame:.4f}s for N={len(robots)} robots"
        )
        self.get_logger().info(
            f"             (base={base_delay} slot={slot} off={tx_offset} jitter={jitter_s:.4f} guard={margin})"
        )

    def _log_cfg(self, as_server: bool):
        robots_expr = str(self.get_parameter("robots").value)
        robots = parse_robots(robots_expr)
        frame = self._frame_duration()
        self._log_auto_frame_if_needed(robots, frame)
        bw_code = parse_bw_to_code(str(self.get_parameter("bw").value))
        slot = float(self.get_parameter("slot").value)
        base_delay = float(self.get_parameter("base_delay").value)
        tx_offset = float(self.get_parameter("tx_offset").value)
        if as_server:
            master = int(self.get_parameter("master").value)
            self.get_logger().info(
                f"[CFG] PORT={self.cfg.port} BAUD={self.cfg.baud} MASTER={master} "
                f"sf={self.cfg.sf} bw_code={bw_code} cr={self.cfg.cr_code} pre={self.cfg.preamble} "
                f"slot={slot:.3f}s base={base_delay:.3f}s off={tx_offset:.3f}s "
                f"frame={frame:.4f}s robots={robots_expr}"
            )
        else:
            master = int(self.get_parameter("master").value)
            self.get_logger().info(
                f"[CFG] PORT={self.cfg.port} BAUD={self.cfg.baud} MY_ID={self.cfg.address} MASTER={master} "
                f"sf={self.cfg.sf} bw_code={bw_code} cr={self.cfg.cr_code} pre={self.cfg.preamble} "
                f"slot={slot:.3f}s base={base_delay:.3f}s off={tx_offset:.3f}s "
                f"rx_delay={float(self.get_parameter('rx_delay_ms').value):.1f}ms "
                f"busy_tail={float(self.get_parameter('busy_tail_ms').value):.1f}ms"
            )

    def _set_lora_address(self, new_address: int) -> bool:
        if self.ser is None:
            return False
        raw_lines: Optional[list[str]] = [] if self.debug_raw_uart else None
        prev_timeout = self.ser.timeout
        # Use blocking line reads during AT re-address to avoid fragmented tokens in logs.
        self.ser.timeout = 0.1
        try:
            lines = at_collect(self.ser, f"AT+ADDRESS={new_address}", 0.6, raw_lines=raw_lines)
            ok, verify_lines = at_expect_ok(self.ser, f"AT+ADDRESS={new_address}", 0.8, raw_lines=raw_lines)
        finally:
            self.ser.timeout = prev_timeout
        msg = f"Set address={new_address}: lines={_fmt_lines(lines)} verify_ok={ok} verify={_fmt_lines(verify_lines)}"
        if raw_lines is not None:
            msg += f" raw_uart={_fmt_raw_lines(raw_lines)}"
        self.get_logger().info(msg)
        if ok:
            self.cfg.address = int(new_address)
        return ok

    def _init_serial(self):
        if serial is None:
            self.get_logger().error("pyserial is not installed; LoRa bridge is disabled.")
            return
        try:
            self.ser = serial.Serial(self.cfg.port, self.cfg.baud, timeout=0.1)
            ok, detail = init_radio(self.ser, self.cfg, debug_raw_uart=self.debug_raw_uart)
            if ok:
                self.get_logger().info(
                    f"LoRa ready on {self.cfg.port} @ {self.cfg.baud} (addr={self.cfg.address}, band={self.cfg.band})"
                )
                self.get_logger().info(f"LoRa init detail: {detail}")
                # Non-blocking reads for ROS timer loop to avoid callback backlog.
                self.ser.timeout = 0
            else:
                self.get_logger().error(f"LoRa init failed. Detail: {detail}")
        except Exception as exc:
            self.ser = None
            self.get_logger().error(f"Cannot open LoRa serial port {self.cfg.port}: {exc}")

    def _start_mode(self):
        if self.ser is None:
            return
        mode = str(self.get_parameter("tdma_mode").value).strip().lower()
        if mode == "server":
            self._switch_to_server()
        elif mode == "client":
            self._switch_to_client(auto_failover=False)
        elif mode == "auto_role":
            self._switch_to_discovery_auto_role()
        else:
            self._switch_to_auto_id()

    def _switch_to_server(self):
        master_id = int(self.get_parameter("master").value)
        if self.cfg.address != master_id:
            self._set_lora_address(master_id)
        self.message_fleet_transmit = "0:0"
        self._last_fleet_receive_key = None
        self.get_logger().info(f"====== Switching to SERVER mode (ID {master_id}) ======")
        self.role = "server"
        self.auto_failover = False
        self.frame_id = 0
        self.next_frame_start = time.monotonic() + 0.2
        self.pending_offer_uuid = None
        self.pending_offer_id = None
        self.pending_offer_ttl = 0
        self._reset_server_stats()
        self._log_cfg(as_server=True)
        self.get_logger().info(f"Switched role -> SERVER (id={self.cfg.address})")

    def _reset_server_stats(self):
        robots = self._robot_list()
        self.per_expected = {r: 0 for r in robots}
        self.per_ok = {r: 0 for r in robots}
        self.per_mismatch = {r: 0 for r in robots}
        self.robot_order = {rid: i for i, rid in enumerate(sorted(robots))}
        self.slot_offset_stats = {r: Stats() for r in robots}
        self.beacon_to_rx_stats = {r: Stats() for r in robots}
        self.last_heard_mono = {r: 0.0 for r in robots}
        self.joined_at_frame = {r: -1 for r in robots}
        self.peer_table = {}
        self.last_server_frame_start_mono = time.monotonic()
        self.next_report_mono = time.monotonic() + float(self.get_parameter("report_interval_s").value)

    def _update_peer_table(
        self,
        peer_id: int,
        *,
        role: str,
        x: int,
        y: int,
        heading: int,
        status: int,
        seq: int,
        rssi: int,
        snr: int,
        src_addr: int,
    ):
        self.peer_table[peer_id] = {
            "role": role,
            "x": x,
            "y": y,
            "heading": heading,
            "status": status,
            "seq": seq,
            "last_update_mono": time.monotonic(),
            "rssi": rssi,
            "snr": snr,
            "src_addr": src_addr,
        }

    def _log_peer_table(self):
        stale_timeout_s = float(self.get_parameter("peer_timeout_s").value)
        robots = self._robot_list()
        master_id = int(self.get_parameter("master").value)
        self.get_logger().info("---- PEER TABLE ----")
        now_m = time.monotonic()
        id_set = set(self.peer_table.keys())
        id_set.update(robots)
        id_set.add(master_id)
        if not id_set:
            self.get_logger().info("(empty)")
            self.get_logger().info("--------------------")
            return
        for peer_id in sorted(id_set):
            peer = self.peer_table.get(peer_id)
            if peer is None:
                role = "master" if peer_id == master_id else "robot"
                age = 9999.0
                seq_text = "--"
                x = y = heading = status_val = rssi = snr = 0
            else:
                role = str(peer["role"])
                age = now_m - float(peer["last_update_mono"])
                seq_val = peer.get("seq")
                seq_text = f"{seq_val:02X}" if isinstance(seq_val, int) else "--"
                x = int(peer["x"])
                y = int(peer["y"])
                heading = int(peer["heading"])
                status_val = int(peer["status"])
                rssi = int(peer["rssi"])
                snr = int(peer["snr"])
            stale = "STALE" if age > stale_timeout_s else "OK"
            self.get_logger().info(
                f"ID {peer_id:>3} [{role:^6}] x={x} y={y} hdg={heading} st={status_val} "
                f"seq={seq_text} RSSI={rssi} SNR={snr} age={age:.1f}s {stale}"
            )
        self.get_logger().info("--------------------")

    def _log_per_summary(self):
        robots = self._robot_list()
        master_id = int(self.get_parameter("master").value)
        self.get_logger().info("====== PER SUMMARY ======")
        total_ok = 0
        total_expected = 0
        for rid in robots:
            if rid == master_id or self.joined_at_frame.get(rid, -1) == -1:
                continue
            expected = self.per_expected.get(rid, 0)
            ok = self.per_ok.get(rid, 0)
            per = (expected - ok) / expected if expected else 0.0
            self.get_logger().info(
                f"Robot {rid}: OK={ok}/{expected} PER={per*100:.3f}% mismatch={self.per_mismatch.get(rid, 0)}"
            )
            self.get_logger().info(f"  BEACON_TO_RX: {self.beacon_to_rx_stats.get(rid, Stats()).fmt()}")
            self.get_logger().info(f"  SLOT_OFFSET : {self.slot_offset_stats.get(rid, Stats()).fmt()}")
            total_ok += ok
            total_expected += expected
        global_per = 1.0 - (total_ok / total_expected) if total_expected else 0.0
        self.get_logger().info(f"GLOBAL PER = {global_per*100:.3f}%")
        self._log_peer_table()

    def _maybe_log_server_reports(self, now_m: float):
        interval_s = max(0.5, float(self.get_parameter("report_interval_s").value))
        if now_m < self.next_report_mono:
            return
        self._log_per_summary()
        self.next_report_mono = now_m + interval_s

    def _switch_to_client(self, auto_failover: bool):
        self.role = "client"
        self.auto_failover = auto_failover
        self.last_frame = -1
        self.last_beacon_mono = time.monotonic()
        self.get_logger().info("====== Switching to CLIENT mode ======")
        self._log_cfg(as_server=False)
        self.get_logger().info(
            f"Switched role -> CLIENT (id={self.cfg.address}, auto_failover={self.auto_failover})"
        )

    def _switch_to_discovery_auto_role(self):
        self.role = "discovery_auto_role"
        self.discovery_timeout_s = float(self.get_parameter("listen_timeout").value) + random.uniform(0.0, 2.0)
        self.discovery_deadline_mono = time.monotonic() + self.discovery_timeout_s
        self.get_logger().info(
            f"[*] Listening for existing master beacons for {self.discovery_timeout_s:.1f}s (jittered)..."
        )
        self.get_logger().info(f"Switched role -> AUTO_ROLE discovery (timeout={self.discovery_timeout_s:.1f}s)")

    def _switch_to_auto_id(self):
        self.role = "auto_id"
        self.auto_id_seen_beacon = False
        self.join_cooldown_frames = random.randint(1, 3)
        self.discovery_timeout_s = float(self.get_parameter("listen_timeout").value) + random.uniform(0.0, 2.0)
        self.discovery_deadline_mono = time.monotonic() + self.discovery_timeout_s
        temp_id = random.randint(30000, 60000)
        self.get_logger().info(f"[*] Starting Auto-ID process. My UUID is {self.my_uuid}")
        self.get_logger().info(f"[*] Initial radio setup with temporary ID {temp_id}")
        if self._set_lora_address(temp_id):
            self.get_logger().info(f"Switched role -> AUTO_ID (uuid={self.my_uuid}, temp_id={temp_id})")
        else:
            self.get_logger().warning("AUTO_ID failed to set temporary address.")
        self.get_logger().info(
            f"[*] Listening for existing master beacons for {self.discovery_timeout_s:.1f}s (jittered)..."
        )

    def fleet_transmit_callback(self, msg: String):
        # Print every received message from /fleet_transmit.
        self.get_logger().info(f"Received /fleet_transmit: {msg.data}")

        parsed = extract_id_data(msg.data)
        if parsed is None:
            self.get_logger().warning('Parse failed. Expected JSON with "id" and "d".')
            return

        id_val, data_val = parsed
        new_payload = f"{id_val}:{data_val}"
        if new_payload == self._last_fleet_transmit_rx:
            self.get_logger().info(
                f"Dropped duplicate /fleet_transmit payload: {new_payload}"
            )
            return
        self._last_fleet_transmit_rx = new_payload
        self.message_fleet_transmit = new_payload
        self.get_logger().info(f"Parsed id={id_val}, data={data_val}")
        self.get_logger().info(f"Updated message_fleet_transmit={self.message_fleet_transmit} for LoRa TX")

    def _fleet_payload(self) -> str:
        return self.message_fleet_transmit or "0:0"

    def _flush_pending_fleet_payload(self, tx_tag: str) -> bool:
        payload = self._fleet_payload()
        if payload == "0:0":
            return False
        sent_ok = self.send_lora(payload, dest=0)
        if sent_ok:
            # One-shot fleet payload: clear after successful LoRa transmission.
            self.message_fleet_transmit = "0:0"
            self.get_logger().info(f"{tx_tag} fleet payload sent: {payload}")
        return sent_ok

    def _publish_fleet_receive_from_lora(self, id_val: str, data_val: str):
        key = f"{id_val.lower()}:{data_val}"
        if key == self._last_fleet_receive_key:
            return
        self._last_fleet_receive_key = key
        outbound = String()
        outbound.data = json.dumps({"v": 2, "id": id_val.lower(), "d": data_val})
        self.publisher_.publish(outbound)
        self.get_logger().info(f"Published /fleet_receive from LoRa: {outbound.data}")

    def send_lora(self, payload: str, dest: int = 0) -> bool:
        if self.ser is None:
            self.get_logger().warning("LoRa serial not available, skip AT+SEND.")
            return False
        cmd = f"AT+SEND={dest},{len(payload)},{payload}"
        try:
            write_cmd(self.ser, cmd)
            self.get_logger().info(f"LoRa TX cmd: {cmd}")
            return True
        except Exception as exc:
            self.get_logger().error(f"LoRa TX failed: {exc}")
            return False

    def poll_lora_callback(self):
        if self.ser is None:
            return
        try:
            self._tick_tdma_state()
            for line in self._drain_serial_lines(max_lines=80):
                parsed = parse_rcv(line)
                if parsed is None:
                    # Ignore noisy/aux lines unless they look meaningful.
                    if self.log_rx and line not in ("OK",) and not line.startswith("+"):
                        self.get_logger().info(f"LoRa RX aux: {line}")
                    continue
                src, _ln, data, rssi, snr = parsed
                if self.log_rx:
                    self.get_logger().info(
                        f"LoRa RX parsed src={src} data={data} RSSI={rssi} SNR={snr}"
                    )
                self._handle_tdma_rx(src, data, rssi, snr)
        except Exception as exc:
            self.get_logger().error(f"LoRa RX polling failed: {exc}")

    def _drain_serial_lines(self, max_lines: int = 80) -> List[str]:
        if self.ser is None:
            return []
        waiting = int(getattr(self.ser, "in_waiting", 0) or 0)
        if waiting <= 0:
            return []

        chunk = self.ser.read(waiting).decode(errors="ignore")
        if not chunk:
            return []

        # Normalise CR/LF boundaries and keep trailing partial line in buffer.
        self._rx_buffer += chunk.replace("\r", "\n")
        parts = self._rx_buffer.split("\n")
        self._rx_buffer = parts[-1]
        lines = [p.strip() for p in parts[:-1] if p.strip()]
        if len(lines) > max_lines:
            lines = lines[-max_lines:]
        return lines

    def _tick_tdma_state(self):
        now_m = time.monotonic()

        if self.role == "server":
            if now_m < self.next_frame_start:
                return
            frame_dur = self._frame_duration()
            while now_m >= self.next_frame_start:
                self.next_frame_start += frame_dur

            self.frame_id = 1 if self.frame_id >= 9999 else self.frame_id + 1
            self.last_server_frame_start_mono = time.monotonic()
            beacon_payload = self._fleet_payload()
            if beacon_payload == "0:0":
                beacon_payload = ""
            beacon = make_beacon(
                frame_id=self.frame_id,
                uuid=self.my_uuid,
                payload=beacon_payload,
                offer_uuid=self.pending_offer_uuid,
                offer_id=self.pending_offer_id,
            )
            beacon_sent_ok = self.send_lora(beacon, dest=0)
            if beacon_sent_ok and beacon_payload:
                # One-shot fleet payload in beacon: clear after successful TX.
                self.message_fleet_transmit = "0:0"
                self.get_logger().info(f"SERVER frame={self.frame_id} fleet payload sent in beacon: {beacon_payload}")
            master_id = int(self.get_parameter("master").value)
            self._update_peer_table(
                master_id,
                role="master",
                x=int(self.get_parameter("x").value),
                y=int(self.get_parameter("y").value),
                heading=int(self.get_parameter("heading").value),
                status=int(self.get_parameter("status").value),
                seq=(self.frame_id % 256),
                rssi=0,
                snr=0,
                src_addr=master_id,
            )
            warmup = int(self.get_parameter("warmup").value)
            for rid in self._robot_list():
                if rid == master_id:
                    continue
                if self.joined_at_frame.get(rid, -1) != -1 and self.frame_id > self.joined_at_frame[rid] + warmup:
                    self.per_expected[rid] = self.per_expected.get(rid, 0) + 1
            if self.pending_offer_ttl > 0:
                self.pending_offer_ttl -= 1
                if self.pending_offer_ttl <= 0:
                    self.pending_offer_uuid = None
                    self.pending_offer_id = None
            self._maybe_log_server_reports(now_m)

        elif self.role == "discovery_auto_role":
            if now_m > self.discovery_deadline_mono:
                self.get_logger().info(f"[*] No master detected after {self.discovery_timeout_s:.1f}s.")
                self._switch_to_server()

        elif self.role == "auto_id":
            if (not self.auto_id_seen_beacon) and now_m > self.discovery_deadline_mono:
                self.get_logger().info(f"[*] No master detected after {self.discovery_timeout_s:.1f}s.")
                self._switch_to_server()

        elif self.role == "client" and self.auto_failover:
            frame = self._frame_duration()
            failover_timeout_s = frame * 5.0 + max(0.0, (self.cfg.address - 1) * 6.0)
            if (now_m - self.last_beacon_mono) > failover_timeout_s:
                self.get_logger().warning(
                    f"CLIENT failover: no beacons for {failover_timeout_s:.1f}s, switching to SERVER."
                )
                self._switch_to_server()

    def _handle_tdma_rx(self, src: int, data: str, rssi: int, snr: int):
        fleet_payload = parse_fleet_payload(data)
        if fleet_payload is not None:
            id_val, data_val = fleet_payload
            self._publish_fleet_receive_from_lora(id_val, data_val)

        if self.role == "server":
            self._handle_server_rx(src, data, rssi, snr)
            return
        if self.role == "client":
            self._handle_client_rx(src, data, rssi, snr)
            return
        if self.role == "discovery_auto_role":
            if parse_beacon(data):
                self.get_logger().info(f"AUTO_ROLE: detected beacon from {src}, switching to CLIENT.")
                self._switch_to_client(auto_failover=True)
                self._handle_client_rx(src, data, rssi, snr)
            return
        if self.role == "auto_id":
            self._handle_auto_id_rx(data)

    def _handle_server_rx(self, src: int, data: str, rssi: int, snr: int):
        beacon = parse_beacon(data)
        if beacon is not None:
            other_uuid = beacon["uuid"]
            should_yield = False
            if src < self.cfg.address:
                should_yield = True
            elif src == self.cfg.address and other_uuid and other_uuid < self.my_uuid:
                should_yield = True
            if should_yield:
                self.get_logger().warning(
                    f"SERVER conflict: yielding to id={src} uuid={other_uuid}, switching to AUTO_ID."
                )
                self._switch_to_auto_id()
            return

        if isinstance(data, str) and data.startswith("JOIN:") and len(data) >= 9:
            uuid_str = str(data[5:9])
            robots = self._robot_list()
            lease_timeout_s = float(self.get_parameter("lease_timeout_s").value)
            if uuid_str in self.auto_id_registry:
                assigned_id = self.auto_id_registry[uuid_str]
            else:
                dead_uuids = []
                now_m = time.monotonic()
                for reg_uuid, reg_id in self.auto_id_registry.items():
                    if now_m - self.last_heard_mono.get(reg_id, 0.0) > lease_timeout_s:
                        dead_uuids.append(reg_uuid)
                for dead in dead_uuids:
                    freed_id = self.auto_id_registry.pop(dead, None)
                    if freed_id is None:
                        continue
                    self.per_expected[freed_id] = 0
                    self.per_ok[freed_id] = 0
                    self.per_mismatch[freed_id] = 0
                    self.slot_offset_stats[freed_id] = Stats()
                    self.beacon_to_rx_stats[freed_id] = Stats()
                    self.joined_at_frame[freed_id] = -1
                    self.last_heard_mono[freed_id] = 0.0

                used_ids = set(self.auto_id_registry.values())
                used_ids.add(int(self.get_parameter("master").value))
                for rid in robots:
                    if self.joined_at_frame.get(rid, -1) != -1:
                        if now_m - self.last_heard_mono.get(rid, 0.0) <= lease_timeout_s:
                            used_ids.add(rid)

                assigned_id = None
                for rid in robots:
                    if rid not in used_ids:
                        assigned_id = rid
                        break
                if assigned_id is None:
                    self.get_logger().warning(f"JOIN from {uuid_str} but no ID available in robots={robots}")
                    return
                self.auto_id_registry[uuid_str] = assigned_id

            self.pending_offer_uuid = uuid_str
            self.pending_offer_id = int(assigned_id)
            self.pending_offer_ttl = 3
            self.last_heard_mono[int(assigned_id)] = time.monotonic()
            self.joined_at_frame[int(assigned_id)] = self.frame_id
            self.get_logger().info(f"JOIN accepted uuid={uuid_str} -> id={assigned_id}")
            return

        if self.pending_offer_id is not None and src == self.pending_offer_id:
            self.pending_offer_uuid = None
            self.pending_offer_id = None
            self.pending_offer_ttl = 0

        pos = parse_pos_payload(data)
        if pos and pos["id"] == src:
            self.last_heard_mono[src] = time.monotonic()
            if self.joined_at_frame.get(src, -1) == -1:
                self.joined_at_frame[src] = self.frame_id
            self._update_peer_table(
                src,
                role="robot",
                x=int(pos["x"]),
                y=int(pos["y"]),
                heading=int(pos["heading"]),
                status=int(pos["status"]),
                seq=int(pos["seq"]),
                rssi=rssi,
                snr=snr,
                src_addr=src,
            )

        robots = self._robot_list()
        master_id = int(self.get_parameter("master").value)
        if src not in robots or src == master_id:
            return

        slot_index = self.robot_order.get(src)
        if slot_index is None:
            return
        t_ms = (time.monotonic() - self.last_server_frame_start_mono) * 1000.0
        expected_ms = (
            float(self.get_parameter("base_delay").value)
            + slot_index * float(self.get_parameter("slot").value)
            + float(self.get_parameter("tx_offset").value)
        ) * 1000.0
        slot_offset_ms = t_ms - expected_ms
        self.slot_offset_stats.setdefault(src, Stats()).add(slot_offset_ms)
        self.beacon_to_rx_stats.setdefault(src, Stats()).add(t_ms)

        self.last_heard_mono[src] = time.monotonic()
        if self.joined_at_frame.get(src, -1) == -1:
            self.joined_at_frame[src] = self.frame_id

        warmup = int(self.get_parameter("warmup").value)
        if self.frame_id > self.joined_at_frame[src] + warmup:
            self.per_ok[src] = self.per_ok.get(src, 0) + 1
        if not (pos and pos["id"] == src):
            self._update_peer_table(
                src,
                role="robot",
                x=0,
                y=0,
                heading=0,
                status=0,
                seq=(self.frame_id % 256),
                rssi=rssi,
                snr=snr,
                src_addr=src,
            )

    def _handle_auto_id_rx(self, data: str):
        beacon = parse_beacon(data)
        if beacon is None:
            return
        self.auto_id_seen_beacon = True
        offer_uuid = beacon.get("offer_uuid")
        offer_id = beacon.get("offer_id")
        if offer_uuid == self.my_uuid and isinstance(offer_id, int):
            if self._set_lora_address(offer_id):
                self.get_logger().info(f"AUTO_ID assigned id={offer_id}; switching to CLIENT.")
                self._switch_to_client(auto_failover=True)
            return

        if self.join_cooldown_frames > 0:
            self.join_cooldown_frames -= 1
            return

        frame = beacon["frame"]
        read_m = time.monotonic()
        rx_delay_s = float(self.get_parameter("rx_delay_ms").value) / 1000.0
        beacon_rx_m = read_m - rx_delay_s
        robots = self._robot_list()
        join_tx_time = (
            beacon_rx_m
            + float(self.get_parameter("base_delay").value)
            + len(robots) * float(self.get_parameter("slot").value)
            + float(self.get_parameter("tx_offset").value)
        )
        sleep_until(join_tx_time, busy_tail_s=float(self.get_parameter("busy_tail_ms").value) / 1000.0)
        payload = f"JOIN:{self.my_uuid}"
        self.send_lora(payload, dest=int(self.get_parameter("master").value))
        self.join_cooldown_frames = random.randint(2, 5)
        self.get_logger().info(f"AUTO_ID JOIN sent on frame={frame} uuid={self.my_uuid}")

    def _handle_client_rx(self, _src: int, data: str, rssi: int, snr: int):
        beacon = parse_beacon(data)
        if beacon is None:
            return

        fleet_payload = parse_fleet_payload(str(beacon.get("payload", "")))
        if fleet_payload is not None:
            id_val, data_val = fleet_payload
            self._publish_fleet_receive_from_lora(id_val, data_val)

        frame = beacon["frame"]
        self.last_beacon_mono = time.monotonic()
        if frame == self.last_frame:
            return
        self.last_frame = frame

        robots = parse_robots(str(self.get_parameter("robots").value))
        my_id = self.cfg.address
        if my_id not in robots:
            self.get_logger().warning(
                f"address={my_id} not in robots={robots}; skip TDMA slot TX."
            )
            return

        robot_order: Dict[int, int] = {rid: i for i, rid in enumerate(sorted(robots))}
        slot_index = robot_order[my_id]

        rx_delay_s = float(self.get_parameter("rx_delay_ms").value) / 1000.0
        base_delay = float(self.get_parameter("base_delay").value)
        slot = float(self.get_parameter("slot").value)
        tx_offset = float(self.get_parameter("tx_offset").value)
        busy_tail_s = float(self.get_parameter("busy_tail_ms").value) / 1000.0
        late_drop_s = float(self.get_parameter("late_drop_ms").value) / 1000.0

        read_m = time.monotonic()
        beacon_rx_m = read_m - rx_delay_s
        tx_time = beacon_rx_m + base_delay + slot_index * slot + tx_offset

        now_m = time.monotonic()
        if now_m > tx_time + late_drop_s:
            late_ms = (now_m - beacon_rx_m) * 1000.0
            self.get_logger().warning(
                f"TDMA skip late frame={frame} now-beacon={late_ms:.1f}ms threshold={late_drop_s*1000:.1f}ms"
            )
            return

        sleep_until(tx_time, busy_tail_s=busy_tail_s)

        if self._fleet_payload() == "0:0":
            self.get_logger().info(
                f"TDMA payload skipped frame={frame}: fleet payload is default 0:0"
            )
            return
        self._flush_pending_fleet_payload(f"CLIENT frame={frame}")
        after_ms = (time.monotonic() - beacon_rx_m) * 1000.0
        self.get_logger().info(
            f"TDMA payload sent frame={frame} after_beacon={after_ms:.1f}ms RSSI={rssi} SNR={snr}"
        )

    def destroy_node(self):
        if self.ser is not None:
            try:
                self.ser.close()
            except Exception:
                pass
        super().destroy_node()


def main(args=None):
    argv = list(sys.argv[1:] if args is None else args)
    cli_parser = argparse.ArgumentParser(add_help=False)
    cli_parser.add_argument("--log", action="store_true", default=False)
    cli_parser.add_argument("--log-rx", action="store_true", default=False)
    cli_parser.add_argument("--log-calc", action="store_true", default=False)
    known, remaining = cli_parser.parse_known_args(argv)

    rclpy.init(args=remaining)
    node = LoraRylr993Node(log_cli=known.log, log_rx_cli=known.log_rx, log_calc_cli=known.log_calc)
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
