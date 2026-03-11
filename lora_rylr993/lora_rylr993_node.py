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


def write_cmd(ser, cmd: str):
    ser.write((cmd + "\r\n").encode())


def at_expect_ok(ser, cmd: str, wait_s: float) -> bool:
    write_cmd(ser, cmd)
    end = time.time() + wait_s
    while time.time() < end:
        line = ser.readline().decode(errors="ignore").strip()
        if line == "OK":
            return True
    return False


def wait_ready(ser, timeout_s: float) -> bool:
    end = time.time() + timeout_s
    while time.time() < end:
        line = ser.readline().decode(errors="ignore").strip()
        if "+READY" in line:
            return True
    return False


def init_radio(ser, cfg: "LoraConfig") -> bool:
    # Basic modem handshake and configuration sequence from lora_tdma.py.
    if not at_expect_ok(ser, "AT", 0.5):
        write_cmd(ser, "ATZ")
        if not wait_ready(ser, 4.0):
            return False
        if not at_expect_ok(ser, "AT", 0.6):
            return False

    bw_code = parse_bw_to_code(cfg.bw)
    params = [
        f"AT+ADDRESS={cfg.address}",
        f"AT+NETWORKID={cfg.network_id}",
        f"AT+PARAMETER={cfg.sf},{bw_code},{cfg.cr_code},{cfg.preamble}",
        f"AT+CRFOP={cfg.tx_power}",
    ]
    for cmd in params:
        at_expect_ok(ser, cmd, 0.5)
    return True


def parse_rcv(line: str) -> Optional[Tuple[int, int, str, int, int]]:
    # +RCV=<src>,<len>,<data>,<rssi>,<snr>
    if not line.startswith("+RCV="):
        return None
    try:
        body = line[5:]
        src_s, len_s, data, rssi_s, snr_s = body.split(",", 4)
        src = int(src_s)
        ln = int(len_s)
        rssi = int(rssi_s)
        snr = int(snr_s)
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
    baud: int = 115200
    address: int = 1
    network_id: int = 18
    sf: int = 7
    bw: str = "125"
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
        self.declare_parameter("baud", 115200)
        self.declare_parameter("address", 1)
        self.declare_parameter("network_id", 18)
        self.declare_parameter("sf", 7)
        self.declare_parameter("bw", "125")
        self.declare_parameter("cr_code", 1)
        self.declare_parameter("preamble", 12)
        self.declare_parameter("tx_power", 22)
        return LoraConfig(
            port=self.get_parameter("port").value,
            baud=int(self.get_parameter("baud").value),
            address=int(self.get_parameter("address").value),
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
            self.ser = serial.Serial(self.cfg.port, self.cfg.baud, timeout=0.05)
            if init_radio(self.ser, self.cfg):
                self.get_logger().info(
                    f"LoRa ready on {self.cfg.port} @ {self.cfg.baud} (addr={self.cfg.address}, net={self.cfg.network_id})"
                )
            else:
                self.get_logger().error("LoRa init failed (AT/ATZ/READY handshake).")
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
