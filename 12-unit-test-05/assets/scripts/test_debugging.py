import pytest
from pyspark.sql import functions as F
from setup_pyspark import get_spark_session

@pytest.fixture(scope="module")
def spark():
    return get_spark_session()

@pytest.fixture
def sample_data(spark):
    data = [("Alice", 29), ("Bob", 31), ("Charlie", 28)]
    schema = ["name", "age"]
    return spark.createDataFrame(data, schema)

def test_incorrect_transformation(spark, sample_data):
    df = sample_data.withColumn("age_double", F.col("age") * 2)
    result = df.filter(F.col("name") == "Alice").collect()[0]
    assert result.age_double == 58  # Intencionalmente errado para debugging