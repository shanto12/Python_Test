import unittest
from main import convert_to_USD, read_input_text

text_path="C:/Personal/Python Test/Python Test (1)/Python Test/input.txt"

class TestTransactions(unittest.TestCase):
    def test_currency_converter(self):
        self.assertAlmostEqual(convert_to_USD("eur", 10),11.696)

    def test_text_reading(self):
        self.assertIsInstance(read_input_text(text_path), str)

