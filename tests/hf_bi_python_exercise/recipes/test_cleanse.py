import unittest
import chispa
import pyspark.sql.functions as F
from hf_bi_python_exercise.spark.session import get_or_create_session
from hf_bi_python_exercise.recipes.cleanse import (
    extract_time_in_minutes,
    categorise_difficulty,
    escape_newlines,
)

spark = get_or_create_session()


class TestCleanse(unittest.TestCase):

    def setUp(self) -> None:
        test_data = [
            ["Meal 1", "PT15M", "Some test meal 1"],
            ["Meal 2", "PT100M4H", "Some test meal 2\n with a newline"],
            ["Meal 3", "PT4H", "Some test meal 3"],
            ["Meal 4", "PT", "Some test meal 4"],
            ["Meal 5", "PT45M", "Some test meal 5"],
        ]

        schema = ["name", "prepTime", "description"]

        self.df_test_data = spark.createDataFrame(test_data, schema)

    def test_extract_time_in_minutes(self) -> None:
        df_actual = extract_time_in_minutes(
            self.df_test_data.select("prepTime"),
            time_col="prepTime",
            output_col="prepTimeMinutes",
        )

        expected_data = [
            ["PT15M", 15.0],
            ["PT100M4H", 340.0],
            ["PT4H", 240.0],
            ["PT", 0.0],
            ["PT45M", 45.0],
        ]

        df_expected = spark.createDataFrame(
            expected_data, ["prepTime", "prepTimeMinutes"]
        )

        chispa.assert_df_equality(df_actual, df_expected)

    def test_categorise_difficulty(self) -> None:
        df_extracted_time = extract_time_in_minutes(
            self.df_test_data.select("prepTime"),
            time_col="prepTime",
            output_col="prepTimeMinutes",
        ).withColumn("cookTimeMinutes", F.lit(0.0))

        df_actual = categorise_difficulty(
            df=df_extracted_time,
            cook_column="cookTimeMinutes",
            prep_column="prepTimeMinutes",
        )

        expected_data = [
            ["PT15M", 15.0, 0.0, "EASY"],
            ["PT100M4H", 340.0, 0.0, "HARD"],
            ["PT4H", 240.0, 0.0, "HARD"],
            ["PT", 0.0, 0.0, "UNKNOWN"],
            ["PT45M", 45.0, 0.0, "MEDIUM"],
        ]

        df_expected = spark.createDataFrame(
            expected_data,
            ["prepTime", "prepTimeMinutes", "cookTimeMinutes", "difficulty"],
        )

        chispa.assert_df_equality(df_actual, df_expected, ignore_nullable=True)

    def test_escape_newlines(self) -> None:
        df_actual = escape_newlines(self.df_test_data, col="description")

        expected_data = [
            ["Meal 1", "PT15M", "Some test meal 1"],
            ["Meal 2", "PT100M4H", "Some test meal 2\\n with a newline"],
            ["Meal 3", "PT4H", "Some test meal 3"],
            ["Meal 4", "PT", "Some test meal 4"],
            ["Meal 5", "PT45M", "Some test meal 5"],
        ]

        schema = ["name", "prepTime", "description"]

        df_expected = spark.createDataFrame(expected_data, schema)

        # Test that the newlines are replaced as expected
        chispa.assert_df_equality(df_actual, df_expected)

        # Test that the initial dataframe is different
        with self.assertRaises(chispa.DataFramesNotEqualError):
            chispa.assert_df_equality(self.df_test_data, df_actual)
