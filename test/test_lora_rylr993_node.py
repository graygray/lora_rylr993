import unittest
from lora_rylr993.lora_rylr993_node import extract_id_data

class TestLoraRylr993Node(unittest.TestCase):

    def test_extract_id_and_data(self):
        payload = '{"v":2,"id":"bf54","d":"v1QDALr//QIJgFg6"}'
        self.assertEqual(
            extract_id_data(payload),
            ("bf54", "v1QDALr//QIJgFg6"),
        )

    def test_missing_fields_returns_none(self):
        payload = '{"v":2,"id":"bf54"}'
        self.assertIsNone(extract_id_data(payload))

    def test_invalid_json_returns_none(self):
        payload = 'id=bf54,d=v1QDALr//QIJgFg6'
        self.assertIsNone(extract_id_data(payload))

    def test_non_string_values_are_stringified(self):
        payload = '{"v":2,"id":1234,"d":5678}'
        self.assertEqual(
            extract_id_data(payload),
            ("1234", "5678"),
        )

if __name__ == '__main__':
    unittest.main()
