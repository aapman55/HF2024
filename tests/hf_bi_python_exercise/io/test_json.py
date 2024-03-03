from dataclasses import dataclass
import unittest
from unittest import mock

from hf_bi_python_exercise.io.json import download_json_data


@dataclass
class DummyResponse:
    text: str


class TestJson(unittest.TestCase):

    @mock.patch("hf_bi_python_exercise.io.json.requests")
    @mock.patch("hf_bi_python_exercise.io.json.Path")
    def test_download_json_data(self, mock_path, mock_requests) -> None:
        """
        The used functions are all standard libraries.
        So what we can test here is whether the calls and outputs are connected correctly
        :param mock_path:
        :param mock_requests:
        :return:
        """
        mock_requests.get.return_value = DummyResponse(text="{}")

        download_json_data(url="dummy_url", download_file_path="dummy_path")

        mock_requests.get.assert_called_with("dummy_url", timeout=3600)
        mock_path.assert_called_with("dummy_path")
        mock_path.return_value.write_text.assert_called_with("{}")
