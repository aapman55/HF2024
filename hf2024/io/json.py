from pathlib import Path
import requests


def download_json_data(url: str, downloadfile_path: str) -> None:
    resp = requests.get(url)
    Path(downloadfile_path).write_text(resp.text)
