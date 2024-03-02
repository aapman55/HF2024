from pathlib import Path
from sys import path as syspath

# Just to be sure that the utils can be found
syspath.append(Path(".").parent.as_posix())

if __name__ == "__main__":
    from hf_bi_python_exercise.io.json import download_json_data
    from hf_bi_python_exercise.spark.session import get_or_create_session
    from hf_bi_python_exercise.recipes.cleanse import (
        extract_time_in_minutes,
        categorise_difficulty,
    )

    spark = get_or_create_session()
    file_path = "./bi_recipes.json"

    #  Download file
    file_url = "https://bnlf-tests.s3.eu-central-1.amazonaws.com/recipes.json"
    download_json_data(file_url, file_path)

    # Read file
    df_raw = spark.read.json(file_path)

    df_cleansed_1 = extract_time_in_minutes(
        df_raw, time_col="prepTime", output_col="prep_time_minutes"
    )
    df_cleansed_2 = extract_time_in_minutes(
        df_cleansed_1, time_col="cookTime", output_col="cook_time_minutes"
    )
    df_cleansed_3 = categorise_difficulty(
        df_cleansed_2, cook_column="cook_time_minutes", prep_column="prep_time_minutes"
    )

    df_filter_chili = df_cleansed_3.where("ingredients LIKE '%chil%'").distinct()

    #  To get 1 file we use pandas
    df_filter_chili.toPandas().to_csv("./chilies.csv", sep="|")
