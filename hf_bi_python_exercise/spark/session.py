from pyspark.sql import SparkSession


def get_or_create_session(name: str, is_local: bool = True) -> SparkSession:
    if is_local:
        return SparkSession.builder.master("local[2]").appName(name).getOrCreate()

    return SparkSession.builder.appName(name).getOrCreate()
