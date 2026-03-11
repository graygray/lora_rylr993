import unittest
from lora_rylr993.lora_rylr993_node import extract_position_fields

class TestLoraRylr993Node(unittest.TestCase):

    def test_extract_cartesian_from_json(self):
        payload = '{"x":10,"y":20,"heading":90,"status":1}'
        self.assertEqual(
            extract_position_fields(payload),
            {"x": 10, "y": 20, "heading": 90, "status": 1},
        )

    def test_extract_gps_from_json(self):
        payload = '{"latitude":25.03,"longitude":121.56,"altitude":44}'
        self.assertEqual(
            extract_position_fields(payload),
            {"lat": 25.03, "lon": 121.56, "alt": 44},
        )

    def test_extract_from_key_value_text(self):
        payload = "x=11,y=22,heading=180,status=2"
        self.assertEqual(
            extract_position_fields(payload),
            {"x": 11, "y": 22, "heading": 180, "status": 2},
        )

    def test_missing_position_returns_none(self):
        payload = '{"speed":4.2,"battery":88}'
        self.assertIsNone(extract_position_fields(payload))

if __name__ == '__main__':
    unittest.main()
