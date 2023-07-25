import pytest
import chispa
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from codac import custom_filter, custom_rename


@pytest.fixture
def spark():
    return SparkSession.builder.master("local").getOrCreate()


@pytest.fixture
def source_data():
    return [
        ("name1", "surname1", "country1"),
        ("name2", "surname2", "country1"),
        ("name3", "surname3", "country3"),
        ("name4", "surname4", "country4"),
    ]


@pytest.fixture
def source_schema():
    return StructType(
        [
            StructField("name", StringType()),
            StructField("surname", StringType()),
            StructField("country", StringType()),
        ]
    )


@pytest.fixture
def df_source(spark, source_data, source_schema):
    return spark.createDataFrame(source_data, source_schema)


def test_custom_filter_1_country(spark, df_source, source_schema):
    df_expected = spark.createDataFrame(
        [
            ("name1", "surname1", "country1"),
            ("name2", "surname2", "country1"),
            ("name4", "surname4", "country4"),
        ],
        source_schema,
    )

    chispa.assert_df_equality(
        custom_filter(df_source, {"country": ["country1", "country4"]}), df_expected
    )


def test_custom_filter_2_countries(spark, df_source, source_schema):
    df_expected = spark.createDataFrame(
        [("name1", "surname1", "country1"), ("name2", "surname2", "country1")],
        source_schema,
    )

    chispa.assert_df_equality(
        custom_filter(df_source, {"country": "country1"}), df_expected
    )


def test_custom_rename(spark, source_data, df_source):
    dfE_expected = spark.createDataFrame(source_data, ["firstname", "lastname", "country"])

    chispa.assert_df_equality(
        custom_rename(
            df=df_source, rename_map={"name": "firstname", "surname": "lastname"}
        ),
        df_expected,
    )
