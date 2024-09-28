import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F
from setup_pyspark import get_spark_session

@pytest.fixture(scope="module")
def spark():
    return get_spark_session()

@pytest.fixture
def nested_data(spark):
    data = [
        Row(id=1, info=Row(name="Alice", age=29, scores=[100, 85])),
        Row(id=2, info=Row(name="Bob", age=31, scores=[95, 88]))
    ]
    return spark.createDataFrame(data)

def test_struct_access(spark, nested_data):
    df = nested_data.select("info.name", "info.age")
    result = df.filter(F.col("name") == "Alice").collect()[0]
    assert result.age == 29

def test_array_access(spark, nested_data):
    df = nested_data.withColumn("first_score", F.col("info.scores")[0])
    result = df.filter(F.col("id") == 1).select("first_score").collect()[0]
    assert result.first_score == 100

def test_array_functions(spark, nested_data):
    df = nested_data.withColumn("score_sum", F.expr("aggregate(info.scores, 0, (x, y) -> x + y)"))
    result = df.filter(F.col("id") == 1).select("score_sum").collect()[0]
    assert result.score_sum == 185
