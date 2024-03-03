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
        escape_newlines,
    )
    import pyspark.sql.functions as F

    spark = get_or_create_session()
    file_path = "./bi_recipes.json"
    output_folder = "./recipes-etl"

    # Create folder if not exists
    Path(output_folder).mkdir(parents=True, exist_ok=True)

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

    # Output Chilies.csv
    # To get 1 file we use pandas
    df_filter_chili_output = escape_newlines(df_filter_chili, "ingredients")
    df_filter_chili_output = escape_newlines(df_filter_chili_output, "description")

    df_filter_chili_output.toPandas().to_csv(
        f"{output_folder}/Chilies.csv", sep="|", index=False
    )

    # Assumption made here is that we continue with the chili dataframe
    # Assumption that AverageTotalTime should not be added as a literal column
    #   But rather that it should be the column name.
    # Assumption that the total time is measured in the lowest granularity, which are minutes.
    df_total_avg_time = (
        df_filter_chili.withColumn(
            "totalTime", F.expr("cook_time_minutes + prep_time_minutes")
        )
        .select("difficulty", "totalTime")
        .groupBy("difficulty")
        .agg(F.avg("totalTime").alias("AverageTotalTime"))
        .withColumn("AverageTotalTime", F.round(F.col("AverageTotalTime")))
        .where("difficulty <> 'UNKNOWN'")
    )

    df_total_avg_time.toPandas().to_csv(
        f"{output_folder}/Results.csv", sep="|", index=False
    )
