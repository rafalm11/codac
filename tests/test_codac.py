import pytest
import chispa
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from codac import customFilter, customRename


@pytest.fixture
def spark():
    return SparkSession.builder.master("local").getOrCreate()


@pytest.fixture
def sourceData():
    return [
        ("name1", "surname1", "country1"),
        ("name2", "surname2", "country1"),
        ("name3", "surname3", "country3"),
        ("name4", "surname4", "country4"),
    ]


@pytest.fixture
def sourceSchema():
    return StructType(
        [
            StructField("name", StringType()),
            StructField("surname", StringType()),
            StructField("country", StringType()),
        ]
    )


@pytest.fixture
def dfSource(spark, sourceData, sourceSchema):
    return spark.createDataFrame(sourceData, sourceSchema)


def test_customFilter_1country(spark, dfSource, sourceSchema):
    dfExpected = spark.createDataFrame(
        [
            ("name1", "surname1", "country1"),
            ("name2", "surname2", "country1"),
            ("name4", "surname4", "country4"),
        ],
        sourceSchema,
    )

    chispa.assert_df_equality(
        customFilter(dfSource, {"country": ["country1", "country4"]}), dfExpected
    )


def test_custom_filter_2countries(spark, dfSource, sourceSchema):
    dfExpected = spark.createDataFrame(
        [("name1", "surname1", "country1"), ("name2", "surname2", "country1")],
        sourceSchema,
    )

    chispa.assert_df_equality(
        customFilter(dfSource, {"country": "country1"}), dfExpected
    )


def test_customRename(spark, sourceData, dfSource):
    dfExpected = spark.createDataFrame(sourceData, ["firstname", "lastname", "country"])

    chispa.assert_df_equality(
        customRename(
            df=dfSource, renameMap={"name": "firstname", "surname": "lastname"}
        ),
        dfExpected,
    )
