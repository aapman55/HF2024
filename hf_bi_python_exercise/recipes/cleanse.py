from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from hf_bi_python_exercise.spark.session import get_or_create_session


spark = get_or_create_session()


def extract_time_in_minutes(df: DataFrame, time_col: str, output_col: str) -> DataFrame:
    """
    The time spent columns have the format PTxxHxxM. They can be both present or none at all.
    This function extracts the numbers based on the letter that is behind the number.
    """
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


def categorise_difficulty(
    df: DataFrame, cook_column: str, prep_column: str
) -> DataFrame:
    """
    There are 4 categories: EASY, MEDIUM, HARD and UNKNOWN.
    UNKNOWN is assigned when no cookTime and no prepTime is available (0.0)
    """
    return df.withColumn(
        "difficulty",
        F.expr(
            f"""
            CASE WHEN {cook_column} + {prep_column} > 60 THEN 'HARD'
                WHEN {cook_column} + {prep_column} >= 30 THEN 'MEDIUM'
                WHEN {cook_column} + {prep_column} > 0 THEN 'EASY'
                ELSE 'UNKNOWN' END
            """
        ),
    )


def escape_newlines(df: DataFrame, col: str) -> DataFrame:
    """
    When exporting to csv we want to have every row on the same line
    """
    return df.withColumn(col, F.regexp_replace(F.col(col), r"\n", r"\\n"))
