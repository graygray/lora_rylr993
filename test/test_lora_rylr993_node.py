import unittest
from lora_rylr993.lora_rylr993_node import (
    extract_id_data,
    parse_beacon,
    parse_pos_payload,
    parse_rcv,
)

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

    def test_parse_rcv_ok(self):
        line = "+RCV=12,5,hello,-34,10"
        self.assertEqual(parse_rcv(line), (12, 5, "hello", -34, 10))

    def test_parse_rcv_bad_len(self):
        line = "+RCV=12,7,hello,-34,10"
        self.assertIsNone(parse_rcv(line))

    def test_parse_rcv_data_with_commas(self):
        line = "+RCV=1,10,ABC,DE,FG,-36,8"
        self.assertEqual(parse_rcv(line), (1, 10, "ABC,DE,FG", -36, 8))

    def test_parse_beacon_with_offer(self):
        data = "BCN,0012,77BE,D6AB:3,P:1:0C:10:20:30:1"
        parsed = parse_beacon(data)
        self.assertEqual(parsed["frame"], 12)
        self.assertEqual(parsed["uuid"], "77BE")
        self.assertEqual(parsed["offer_uuid"], "D6AB")
        self.assertEqual(parsed["offer_id"], 3)
        self.assertEqual(parsed["master_pos"]["id"], 1)

    def test_parse_pos_payload(self):
        data = "POS,2,0A,100,200,90,1"
        self.assertEqual(
            parse_pos_payload(data),
            {"id": 2, "seq": 10, "x": 100, "y": 200, "heading": 90, "status": 1},
        )

if __name__ == '__main__':
    unittest.main()
