from pathlib import Path
import requests


def download_json_data(url: str, download_file_path: str) -> None:
    # To prevent indefinite hanging if the server is not responsive
    # The request will timeout after an hour
    resp = requests.get(url, timeout=3600)
    Path(download_file_path).write_text(resp.text)
