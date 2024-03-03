from pyspark.sql import SparkSession


def get_or_create_session(is_local: bool = True) -> SparkSession:
    """
    Creates a spark session with a standard app name. This way we can get the same spark session
    from the different modules.

    If we are local we assign 2 cpu cores for spark.
    """
    if is_local:
        return (
            SparkSession.builder.master("local[2]").appName("HelloFresh").getOrCreate()
        )

    return SparkSession.builder.appName("HelloFresh").getOrCreate()
