from json import loads as json_loads
from sys import argv
from typing import Dict, List
from pyspark.sql import DataFrame, SparkSession
from utils import get_config, get_parameters, logger_init


logger = logger_init(__name__)


def custom_filter(df: DataFrame, filter_map: Dict[str, List[str]]) -> DataFrame:
    """Filter DataFrame using filter_map."""
    logger.debug(f"custom_filter started. filter_map:{filter_map}")
    for column, values_list in filter_map.items():
        df = df.filter(df[column].isin(values_list))
        logger.debug(f"filter by column:{column} with values:{values_list}")
    return df


def custom_rename(df: DataFrame, rename_map: Dict[str, str]) -> DataFrame:
    """Rename DataFrame column using rename_map."""
    logger.debug(f"custom_rename started. rename_map:{rename_map}")
    df = df.withColumnsRenamed(rename_map)
    return df


def main():
    """start codacJoiner app that joins two input files into one filtering by country"""
    logger.info("main started")
    personal_fileName, accounts_fileName, country_filter = get_parameters(argv[1:])
    logger.debug(
        f"parameters. p:{personal_fileName} a:{accounts_fileName} c:{country_filter}"
    )

    config = get_config("codac.ini")

    spark_session = (
        SparkSession.builder.appName("codac")
        .master(config["SPARK"]["sparkUrl"])
        .getOrCreate()
    )
    logger.debug("spark session created")

    df_personal = spark_session.read.csv(path=personal_fileName, header=True)
    logger.info(f"opened Personal DataFrame from file:{personal_fileName}")

    df_accounts = spark_session.read.csv(path=accounts_fileName, header=True)
    logger.info(f"opened Accounts DataFrame from file:{accounts_fileName}")

    df_out = df_personal.select("id", "email", "country")
    logger.debug("created Out Dataframe with removed columns from Personal DataFrame")

    df_out = custom_filter(df_out, {"country": country_filter})
    logger.debug(
        f"filtered columns on Out DataFrame with countryFilter:{country_filter}"
    )

    df_out = df_out.join(df_accounts, on=df_personal.id == df_accounts.id, how="inner")
    logger.debug("joined with Accounts DataFrame")

    df_out = df_out.drop(df_accounts.id)
    logger.debug("removed duplicated join column from Out Dataframe")

    df_out = custom_rename(df_out, json_loads(config["LOGIC"]["renameMap"]))
    logger.debug(
        f'renamed columns on Out DataFrame with renameMap:{json_loads(config["LOGIC"]["renameMap"])}'
    )

    logger.debug("df_out count:" + str(df_out.count()))

    df_out.write.csv(config["SPARK"]["outputFolderName"], header=True, mode="overwrite")
    logger.info(f'saved Out Dataframe to folder:{config["SPARK"]["outputFolderName"]}')


if __name__ == "__main__":
    main()
