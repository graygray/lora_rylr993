import json
import re
from typing import Dict, Optional, Tuple

import rclpy
from rclpy.node import Node
from std_msgs.msg import String


def _first_value(obj: Dict[str, object], aliases: Tuple[str, ...]) -> Optional[object]:
    for key in aliases:
        if key in obj:
            return obj[key]
    return None


def _to_number(value: object) -> Optional[float]:
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _pick_position_fields(obj: Dict[str, object]) -> Optional[Dict[str, float]]:
    key_aliases = {
        'x': ('x',),
        'y': ('y',),
        'heading': ('heading', 'yaw'),
        'status': ('status',),
        'lat': ('lat', 'latitude'),
        'lon': ('lon', 'lng', 'longitude'),
        'alt': ('alt', 'altitude'),
    }

    out: Dict[str, float] = {}
    for canonical, aliases in key_aliases.items():
        val = _first_value(obj, aliases)
        if val is None:
            continue
        num = _to_number(val)
        if num is not None:
            out[canonical] = num

    # Accept cartesian (x/y) OR GPS (lat/lon) as valid position payload.
    has_cartesian = 'x' in out and 'y' in out
    has_gps = 'lat' in out and 'lon' in out
    return out if (has_cartesian or has_gps) else None


def _extract_position_fields_raw(payload: str) -> Optional[Dict[str, float]]:
    # 1) Try JSON first.
    try:
        obj = json.loads(payload)
        if isinstance(obj, dict):
            parsed = _pick_position_fields(obj)
            if parsed is not None:
                return parsed
    except json.JSONDecodeError:
        pass

    # 2) Try key-value text, e.g. "x=1,y=2,heading=90,status=1" or "x:1 y:2".
    text_obj: Dict[str, str] = {}
    for key, value in re.findall(r'([A-Za-z_][A-Za-z0-9_]*)\s*[:=]\s*([^,\s]+)', payload):
        text_obj[key] = value

    if text_obj:
        parsed = _pick_position_fields(text_obj)
        if parsed is not None:
            return parsed

    return None


def _normalize_numeric_types(data: Dict[str, float]) -> Dict[str, object]:
    out: Dict[str, object] = {}
    for key, val in data.items():
        out[key] = int(val) if float(val).is_integer() else val
    return out


def extract_position_fields(payload: str) -> Optional[Dict[str, object]]:
    parsed = _extract_position_fields_raw(payload)
    if parsed is None:
        return None
    return _normalize_numeric_types(parsed)


class LoraRylr993Node(Node):
    def __init__(self):
        super().__init__('lora_rylr993_node')
        self.publisher_ = self.create_publisher(String, '/fleet_receive', 10)
        self.subscription_ = self.create_subscription(
            String,
            '/fleet_transmit',
            self.fleet_transmit_callback,
            10,
        )
        self.get_logger().info(
            'Monitoring /fleet_transmit and publishing position to /fleet_receive'
        )

    def fleet_transmit_callback(self, msg: String):
        pos_fields = extract_position_fields(msg.data)
        if pos_fields is None:
            self.get_logger().warning(
                f'No position fields found in /fleet_transmit payload: "{msg.data}"'
            )
            return

        outbound = String()
        outbound.data = json.dumps(pos_fields, separators=(',', ':'))
        self.publisher_.publish(outbound)
        self.get_logger().info(f'Published /fleet_receive: {outbound.data}')


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


if __name__ == '__main__':
    main()
