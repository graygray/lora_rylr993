import json
import time
from dataclasses import dataclass
from typing import Optional, Tuple

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
    lines = at_collect(ser, f"AT+NETWORKID={cfg.network_id}", 0.6)
    debug.append(f"AT+NETWORKID={cfg.network_id}: lines={_fmt_lines(lines)}")
    lines = at_collect(ser, f"AT+PARAMETER={cfg.sf},{bw_code},{cfg.cr_code},{cfg.preamble}", 0.8)
    debug.append(f"AT+PARAMETER={cfg.sf},{bw_code},{cfg.cr_code},{cfg.preamble}: lines={_fmt_lines(lines)}")
    lines = at_collect(ser, f"AT+CRFOP={cfg.tx_power}", 0.6)
    debug.append(f"AT+CRFOP={cfg.tx_power}: lines={_fmt_lines(lines)}")
    lines = at_collect(ser, "AT+PARAMETER=?", 0.8)
    debug.append(f"AT+PARAMETER=?: lines={_fmt_lines(lines)}")
    drain_uart(ser, 0.8)

    verify_cmds = [f"AT+ADDRESS={cfg.address}", f"AT+NETWORKID={cfg.network_id}"]
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


@dataclass
class LoraConfig:
    port: str = "/dev/ttyUSB0"
    baud: int = 9600
    address: int = 1
    band: int = 915000000
    network_id: int = 18
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
        self.ser = None
        self._init_serial()
        self.poll_timer = self.create_timer(0.05, self.poll_lora_callback)

        self.get_logger().info(
            "Monitoring /fleet_transmit, parsing {id,d}, publishing to /fleet_receive, and bridging LoRa RYLR993"
        )

    def _load_config(self) -> LoraConfig:
        self.declare_parameter("port", "/dev/ttyUSB0")
        self.declare_parameter("baud", 9600)
        self.declare_parameter("address", 1)
        self.declare_parameter("band", 915000000)
        self.declare_parameter("network_id", 18)
        self.declare_parameter("sf", 5)
        self.declare_parameter("bw", "500")
        self.declare_parameter("cr_code", 1)
        self.declare_parameter("preamble", 12)
        self.declare_parameter("tx_power", 22)
        return LoraConfig(
            port=resolve_port(self.get_parameter("port").value),
            baud=int(self.get_parameter("baud").value),
            address=int(self.get_parameter("address").value),
            band=int(self.get_parameter("band").value),
            network_id=int(self.get_parameter("network_id").value),
            sf=int(self.get_parameter("sf").value),
            bw=str(self.get_parameter("bw").value),
            cr_code=int(self.get_parameter("cr_code").value),
            preamble=int(self.get_parameter("preamble").value),
            tx_power=int(self.get_parameter("tx_power").value),
        )

    def _init_serial(self):
        if serial is None:
            self.get_logger().error("pyserial is not installed; LoRa bridge is disabled.")
            return
        try:
            self.ser = serial.Serial(self.cfg.port, self.cfg.baud, timeout=0.1)
            ok, detail = init_radio(self.ser, self.cfg)
            if ok:
                self.get_logger().info(
                    f"LoRa ready on {self.cfg.port} @ {self.cfg.baud} (addr={self.cfg.address}, band={self.cfg.band}, net={self.cfg.network_id})"
                )
                self.get_logger().info(f"LoRa init detail: {detail}")
            else:
                self.get_logger().error(f"LoRa init failed. Detail: {detail}")
        except Exception as exc:
            self.ser = None
            self.get_logger().error(f"Cannot open LoRa serial port {self.cfg.port}: {exc}")

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

    def send_lora(self, payload: str):
        if self.ser is None:
            self.get_logger().warning("LoRa serial not available, skip AT+SEND.")
            return
        cmd = f"AT+SEND=0,{len(payload)},{payload}"
        try:
            write_cmd(self.ser, cmd)
            self.get_logger().info(f"LoRa TX: {payload}")
        except Exception as exc:
            self.get_logger().error(f"LoRa TX failed: {exc}")

    def poll_lora_callback(self):
        if self.ser is None:
            return
        try:
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
        except Exception as exc:
            self.get_logger().error(f"LoRa RX polling failed: {exc}")

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
