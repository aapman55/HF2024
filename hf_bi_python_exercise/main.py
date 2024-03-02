from pathlib import Path
from sys import path as syspath

# Just to be sure that the utils can be found
syspath.append(Path(".").parent.parent.as_posix())

if __name__ == "__main__":
    from hf2024.io.json import download_json_data

    file_url = "https://bnlf-tests.s3.eu-central-1.amazonaws.com/recipes.json"
    download_json_data(file_url, "./bi_recipes.json")
