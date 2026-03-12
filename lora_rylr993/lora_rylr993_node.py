import json
import random
import time
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import rclpy
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


def at_expect_ok(ser, cmd: str, wait_s: float) -> Tuple[bool, list[str]]:
    write_cmd(ser, cmd)
    end = time.time() + wait_s
    lines: list[str] = []
    while time.time() < end:
        line = ser.readline().decode(errors="ignore").strip()
        if line:
            lines.append(line)
        if line == "OK":
            return True, lines
    return False, lines


def at_collect(ser, cmd: str, wait_s: float) -> list[str]:
    write_cmd(ser, cmd)
    end = time.time() + wait_s
    lines: list[str] = []
    while time.time() < end:
        line = ser.readline().decode(errors="ignore").strip()
        if line:
            lines.append(line)
    return lines


def wait_ready(ser, timeout_s: float) -> Tuple[bool, list[str]]:
    end = time.time() + timeout_s
    lines: list[str] = []
    while time.time() < end:
        line = ser.readline().decode(errors="ignore").strip()
        if line:
            lines.append(line)
        if "+READY" in line:
            return True, lines
    return False, lines


def drain_uart(ser, seconds: float):
    end = time.time() + seconds
    while time.time() < end:
        _ = ser.readline().decode(errors="ignore").strip()


def _fmt_lines(lines: list[str], max_items: int = 8) -> str:
    if not lines:
        return "[]"
    if len(lines) <= max_items:
        return "[" + " | ".join(lines) + "]"
    shown = " | ".join(lines[:max_items])
    return f"[{shown} | ... +{len(lines) - max_items} more]"


def init_radio(ser, cfg: "LoraConfig") -> Tuple[bool, str]:
    # Robust modem handshake/config flow aligned with lora_tdma.py.
    debug: list[str] = []

    ok, lines = at_expect_ok(ser, "AT", 0.5)
    debug.append(f"AT: ok={ok} lines={_fmt_lines(lines)}")
    if not ok:
        write_cmd(ser, "ATZ")
        ready_ok, ready_lines = wait_ready(ser, 4.0)
        debug.append(f"ATZ->READY: ok={ready_ok} lines={_fmt_lines(ready_lines)}")
        if not ready_ok:
            return False, " ; ".join(debug)
        ok, lines = at_expect_ok(ser, "AT", 1.0)
        debug.append(f"AT(retry): ok={ok} lines={_fmt_lines(lines)}")
        if not ok:
            return False, " ; ".join(debug)

    write_cmd(ser, "AT+OPMODE=1")
    time.sleep(0.6)
    need_reset = False
    while True:
        line = ser.readline().decode(errors="ignore").strip()
        if not line:
            break
        if "Need RESET" in line:
            need_reset = True

    debug.append(f"AT+OPMODE=1: need_reset={need_reset}")
    if need_reset:
        write_cmd(ser, "ATZ")
        ready_ok, ready_lines = wait_ready(ser, 4.0)
        debug.append(f"ATZ(after Need RESET)->READY: ok={ready_ok} lines={_fmt_lines(ready_lines)}")
        if not ready_ok:
            return False, " ; ".join(debug)

    bw_code = parse_bw_to_code(cfg.bw)
    lines = at_collect(ser, f"AT+ADDRESS={cfg.address}", 0.6)
    debug.append(f"AT+ADDRESS={cfg.address}: lines={_fmt_lines(lines)}")
    lines = at_collect(ser, f"AT+BAND={cfg.band}", 0.6)
    debug.append(f"AT+BAND={cfg.band}: lines={_fmt_lines(lines)}")
    lines = at_collect(ser, f"AT+PARAMETER={cfg.sf},{bw_code},{cfg.cr_code},{cfg.preamble}", 0.8)
    debug.append(f"AT+PARAMETER={cfg.sf},{bw_code},{cfg.cr_code},{cfg.preamble}: lines={_fmt_lines(lines)}")
    lines = at_collect(ser, f"AT+CRFOP={cfg.tx_power}", 0.6)
    debug.append(f"AT+CRFOP={cfg.tx_power}: lines={_fmt_lines(lines)}")
    lines = at_collect(ser, "AT+PARAMETER=?", 0.8)
    debug.append(f"AT+PARAMETER=?: lines={_fmt_lines(lines)}")
    drain_uart(ser, 0.8)

    verify_cmds = [f"AT+ADDRESS={cfg.address}"]
    for cmd in verify_cmds:
        ok, lines = at_expect_ok(ser, cmd, 0.8)
        debug.append(f"verify {cmd}: ok={ok} lines={_fmt_lines(lines)}")
        if not ok:
            return False, " ; ".join(debug)

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
    master_id: int,
    master_x: int,
    master_y: int,
    master_heading: int,
    master_status: int,
    offer_uuid: Optional[str] = None,
    offer_id: Optional[int] = None,
) -> str:
    offer_str = ""
    if offer_uuid and offer_id is not None:
        offer_str = f"{offer_uuid}:{offer_id}"
    pos = make_master_pos_field(
        master_id=master_id,
        seq=(frame_id % 256),
        x=master_x,
        y=master_y,
        heading=master_heading,
        status=master_status,
    )
    return f"BCN,{frame_id:04d},{uuid or ''},{offer_str},{pos}"


def parse_beacon(data: str):
    # BCN,<frame>,<uuid>,<offer>,P:<master_id>:<seq>:<x>:<y>:<heading>:<status>
    if not data.startswith("BCN,"):
        return None
    try:
        parts = data.split(",")
        if len(parts) < 5:
            return None
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
            "master_pos": parse_master_pos_field(parts[4]),
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


class LoraRylr993Node(Node):
    def __init__(self):
        super().__init__("lora_rylr993_node")
        self.publisher_ = self.create_publisher(String, "/fleet_receive", 10)
        self.subscription_ = self.create_subscription(
            String,
            "/fleet_transmit",
            self.fleet_transmit_callback,
            10,
        )

        self.cfg = self._load_config()
        self.my_uuid = f"{random.randint(0, 0xFFFF):04X}"
        self.role = "idle"
        self.auto_failover = False
        self.last_frame = -1
        self.last_beacon_mono = time.monotonic()
        self.discovery_deadline_mono = 0.0
        self.auto_id_seen_beacon = False
        self.join_cooldown_frames = 0
        self.frame_id = 0
        self.next_frame_start = time.monotonic()
        self.pending_offer_uuid: Optional[str] = None
        self.pending_offer_id: Optional[int] = None
        self.pending_offer_ttl = 0
        self.auto_id_registry: Dict[str, int] = {}
        self.last_heard_mono: Dict[int, float] = {}
        self.joined_at_frame: Dict[int, int] = {}
        self.ser = None
        self._init_serial()
        self._start_mode()
        self.poll_timer = self.create_timer(0.05, self.poll_lora_callback)

        self.get_logger().info(
            "Monitoring /fleet_transmit, parsing {id,d}, publishing to /fleet_receive, and running TDMA role logic"
        )

    def _load_config(self) -> LoraConfig:
        self.declare_parameter("port", "/dev/ttyUSB0")
        self.declare_parameter("baud", 9600)
        self.declare_parameter("address", 1)
        self.declare_parameter("band", 915000000)
        self.declare_parameter("sf", 5)
        self.declare_parameter("bw", "500")
        self.declare_parameter("cr_code", 1)
        self.declare_parameter("preamble", 12)
        self.declare_parameter("tx_power", 22)
        # TDMA runtime params aligned to lora_tdma.py defaults.
        self.declare_parameter("tdma_mode", "auto_id")  # server | client | auto_role | auto_id
        self.declare_parameter("master", 1)
        self.declare_parameter("robots", "1-4")
        self.declare_parameter("base_delay", 0.25)
        self.declare_parameter("slot", 0.08)
        self.declare_parameter("tx_offset", 0.016)
        self.declare_parameter("frame", 0.0)  # <=0 means auto-calc
        self.declare_parameter("margin", 0.03)
        self.declare_parameter("assumed_jitter_ms", 1.8)
        self.declare_parameter("rx_delay_ms", 190.0)
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

    def _set_lora_address(self, new_address: int) -> bool:
        if self.ser is None:
            return False
        lines = at_collect(self.ser, f"AT+ADDRESS={new_address}", 0.6)
        ok, verify_lines = at_expect_ok(self.ser, f"AT+ADDRESS={new_address}", 0.8)
        self.get_logger().info(
            f"Set address={new_address}: lines={_fmt_lines(lines)} verify_ok={ok} verify={_fmt_lines(verify_lines)}"
        )
        if ok:
            self.cfg.address = int(new_address)
        return ok

    def _init_serial(self):
        if serial is None:
            self.get_logger().error("pyserial is not installed; LoRa bridge is disabled.")
            return
        try:
            self.ser = serial.Serial(self.cfg.port, self.cfg.baud, timeout=0.1)
            ok, detail = init_radio(self.ser, self.cfg)
            if ok:
                self.get_logger().info(
                    f"LoRa ready on {self.cfg.port} @ {self.cfg.baud} (addr={self.cfg.address}, band={self.cfg.band})"
                )
                self.get_logger().info(f"LoRa init detail: {detail}")
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
        self.role = "server"
        self.auto_failover = False
        self.frame_id = 0
        self.next_frame_start = time.monotonic() + 0.2
        self.pending_offer_uuid = None
        self.pending_offer_id = None
        self.pending_offer_ttl = 0
        self.get_logger().info(f"Switched role -> SERVER (id={self.cfg.address})")

    def _switch_to_client(self, auto_failover: bool):
        self.role = "client"
        self.auto_failover = auto_failover
        self.last_frame = -1
        self.last_beacon_mono = time.monotonic()
        self.get_logger().info(
            f"Switched role -> CLIENT (id={self.cfg.address}, auto_failover={self.auto_failover})"
        )

    def _switch_to_discovery_auto_role(self):
        self.role = "discovery_auto_role"
        timeout_s = float(self.get_parameter("listen_timeout").value) + random.uniform(0.0, 2.0)
        self.discovery_deadline_mono = time.monotonic() + timeout_s
        self.get_logger().info(f"Switched role -> AUTO_ROLE discovery (timeout={timeout_s:.1f}s)")

    def _switch_to_auto_id(self):
        self.role = "auto_id"
        self.auto_id_seen_beacon = False
        self.join_cooldown_frames = random.randint(1, 3)
        timeout_s = float(self.get_parameter("listen_timeout").value) + random.uniform(0.0, 2.0)
        self.discovery_deadline_mono = time.monotonic() + timeout_s
        temp_id = random.randint(30000, 60000)
        if self._set_lora_address(temp_id):
            self.get_logger().info(f"Switched role -> AUTO_ID (uuid={self.my_uuid}, temp_id={temp_id})")
        else:
            self.get_logger().warning("AUTO_ID failed to set temporary address.")

    def fleet_transmit_callback(self, msg: String):
        # Print every received message from /fleet_transmit.
        self.get_logger().info(f"Received /fleet_transmit: {msg.data}")

        parsed = extract_id_data(msg.data)
        if parsed is None:
            self.get_logger().warning('Parse failed. Expected JSON with "id" and "d".')
            return

        id_val, data_val = parsed
        combined = f"{id_val}_{data_val}"

        outbound = String()
        outbound.data = combined
        self.publisher_.publish(outbound)
        self.get_logger().info(f"Parsed id={id_val}, data={data_val}")
        self.get_logger().info(f"Published /fleet_receive: {combined}")

        # Bridge parsed data to LoRa.
        self.send_lora(combined)

    def send_lora(self, payload: str, dest: int = 0):
        if self.ser is None:
            self.get_logger().warning("LoRa serial not available, skip AT+SEND.")
            return
        cmd = f"AT+SEND={dest},{len(payload)},{payload}"
        try:
            write_cmd(self.ser, cmd)
            self.get_logger().info(f"LoRa TX dest={dest}: {payload}")
        except Exception as exc:
            self.get_logger().error(f"LoRa TX failed: {exc}")

    def poll_lora_callback(self):
        if self.ser is None:
            return
        try:
            self._tick_tdma_state()
            for _ in range(20):
                line = self.ser.readline().decode(errors="ignore").strip()
                if not line:
                    break
                if all(ch == "\x00" for ch in line):
                    continue
                self.get_logger().info(f"LoRa RX line: {line}")
                parsed = parse_rcv(line)
                if parsed is None:
                    continue
                src, _ln, data, rssi, snr = parsed
                self.get_logger().info(
                    f"LoRa RX parsed src={src} data={data} RSSI={rssi} SNR={snr}"
                )
                self._handle_tdma_rx(src, data, rssi, snr)
        except Exception as exc:
            self.get_logger().error(f"LoRa RX polling failed: {exc}")

    def _tick_tdma_state(self):
        now_m = time.monotonic()

        if self.role == "server":
            if now_m < self.next_frame_start:
                return
            frame_dur = self._frame_duration()
            while now_m >= self.next_frame_start:
                self.next_frame_start += frame_dur

            self.frame_id = 1 if self.frame_id >= 9999 else self.frame_id + 1
            x = int(self.get_parameter("x").value)
            y = int(self.get_parameter("y").value)
            heading = int(self.get_parameter("heading").value)
            status = int(self.get_parameter("status").value)
            beacon = make_beacon(
                frame_id=self.frame_id,
                uuid=self.my_uuid,
                master_id=self.cfg.address,
                master_x=x,
                master_y=y,
                master_heading=heading,
                master_status=status,
                offer_uuid=self.pending_offer_uuid,
                offer_id=self.pending_offer_id,
            )
            self.send_lora(beacon, dest=0)
            if self.pending_offer_ttl > 0:
                self.pending_offer_ttl -= 1
                if self.pending_offer_ttl <= 0:
                    self.pending_offer_uuid = None
                    self.pending_offer_id = None

        elif self.role == "discovery_auto_role":
            if now_m > self.discovery_deadline_mono:
                self.get_logger().info("AUTO_ROLE: no beacon detected, becoming SERVER.")
                self._switch_to_server()

        elif self.role == "auto_id":
            if (not self.auto_id_seen_beacon) and now_m > self.discovery_deadline_mono:
                self.get_logger().info("AUTO_ID: no beacon detected, becoming SERVER.")
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
        if self.role == "server":
            self._handle_server_rx(src, data)
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

    def _handle_server_rx(self, src: int, data: str):
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
                    self.auto_id_registry.pop(dead, None)

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

        x = int(self.get_parameter("x").value)
        y = int(self.get_parameter("y").value)
        heading = int(self.get_parameter("heading").value)
        status = int(self.get_parameter("status").value)
        payload = make_pos_payload(my_id, frame % 256, x, y, heading, status)
        self.send_lora(payload)
        after_ms = (time.monotonic() - beacon_rx_m) * 1000.0
        self.get_logger().info(
            f"TDMA POS sent frame={frame} after_beacon={after_ms:.1f}ms RSSI={rssi} SNR={snr}"
        )

    def destroy_node(self):
        if self.ser is not None:
            try:
                self.ser.close()
            except Exception:
                pass
        super().destroy_node()


def main(args=None):
    rclpy.init(args=args)
    node = LoraRylr993Node()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.destroy_node()
        rclpy.try_shutdown()


if __name__ == "__main__":
    main()
