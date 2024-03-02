from pathlib import Path
from sys import path as syspath

# Just to be sure that the utils can be found
syspath.append(Path(".").parent.as_posix())

if __name__ == "__main__":
    from hf_bi_python_exercise.io.json import download_json_data
    from hf_bi_python_exercise.spark.session import get_or_create_session
    from hf_bi_python_exercise.recipes.cleanse import extract_time_in_minutes

    spark = get_or_create_session()
    file_path = "./bi_recipes.json"

    #  Download file
    file_url = "https://bnlf-tests.s3.eu-central-1.amazonaws.com/recipes.json"
    download_json_data(file_url, file_path)

    # Read file
    df_raw = spark.read.json(file_path)

    df_cleansed = extract_time_in_minutes(
        df_raw, time_col="prepTime", output_col="prep_time_minutes"
    )
    df_cleansed.show()
