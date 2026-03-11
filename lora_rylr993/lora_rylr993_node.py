import json
from typing import Optional, Tuple

import rclpy
from rclpy.node import Node
from std_msgs.msg import String


def extract_id_data(payload: str) -> Optional[Tuple[str, str]]:
    """Extract id and data from JSON payload: {"v":2,"id":"bf54","d":"..."}."""
    try:
        obj = json.loads(payload)
    except json.JSONDecodeError:
        return None

    if not isinstance(obj, dict):
        return None

    id_val = obj.get('id')
    data_val = obj.get('d')
    if id_val is None or data_val is None:
        return None

    return str(id_val), str(data_val)


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
            'Monitoring /fleet_transmit and publishing id_data string to /fleet_receive'
        )

    def fleet_transmit_callback(self, msg: String):
        # Print every received message for monitoring.
        self.get_logger().info(f'Received /fleet_transmit: {msg.data}')

        parsed = extract_id_data(msg.data)
        if parsed is None:
            self.get_logger().warning(
                'Payload parse failed. Expected JSON with "id" and "d" fields.'
            )
            return

        id_val, data_val = parsed
        combined = f'{id_val}_{data_val}'

        outbound = String()
        outbound.data = combined
        self.publisher_.publish(outbound)
        self.get_logger().info(f'Parsed id={id_val}, data={data_val}')
        self.get_logger().info(f'Published /fleet_receive: {combined}')


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
