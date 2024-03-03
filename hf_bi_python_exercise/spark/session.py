from pyspark.sql import SparkSession


def get_or_create_session(is_local: bool = True) -> SparkSession:
    if is_local:
        return (
            SparkSession.builder.master("local[2]").appName("HelloFresh").getOrCreate()
        )

    return SparkSession.builder.appName("HelloFresh").getOrCreate()
