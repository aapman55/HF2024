from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from hf_bi_python_exercise.spark.session import get_or_create_session


spark = get_or_create_session()


def extract_time_in_minutes(df: DataFrame, time_col: str, output_col: str) -> DataFrame:
    df = (
        df.withColumn(
            "Minutes",
            F.nullif(F.regexp_extract(F.col(time_col), r"(\d+)M", 1), F.lit("")),
        )
        .withColumn(
            "Hours",
            F.nullif(F.regexp_extract(F.col(time_col), r"(\d+)H", 1), F.lit("")),
        )
        .withColumn(
            output_col, F.expr("COALESCE(Minutes, 0) + COALESCE(Hours, 0) * 60")
        )
        .drop("Minutes", "Hours")
    )

    return df
