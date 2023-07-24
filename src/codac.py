from json import loads as json_loads
from sys import argv
from typing import Dict, List
from pyspark.sql import DataFrame, SparkSession
from utils import getConfig, getParameters, loggerInit

    
logger = loggerInit(__name__)




def customFilter(df: DataFrame, filterMap: Dict[str, List[str]]) -> DataFrame:
    logger.debug(f'customFilter started. filter:{filterMap}')
    for column,valuesList in filterMap.items():
        df = df.filter(df[column].isin(valuesList))
        logger.debug(f'filter by column:{column} with values:{valuesList}')
    return df

def customRename(df: DataFrame, renameMap: Dict[str,str]) -> DataFrame:
    logger.debug(f'customRename started. renameMap:{renameMap}')
    df = df.withColumnsRenamed(renameMap)
    return df



def main():
    logger.info('main started')
    personalFileName, accountsFileName, countryFilter  = getParameters(argv[1:])
    logger.debug(f'parameters. p:{personalFileName} a:{accountsFileName} c:{countryFilter}')

    config = getConfig('codac.ini')
    
    sparkSession = SparkSession.builder.appName('codac').master(config['SPARK']['sparkUrl']).getOrCreate()
    logger.debug('spark session created')

    dfPersonal = sparkSession.read.csv(path=personalFileName,header=True)
    logger.info(f'opened Personal DataFrame from file:{personalFileName}')

    dfAccounts = sparkSession.read.csv(path=accountsFileName,header=True)
    logger.info(f'opened Accounts DataFrame from file:{accountsFileName}')

    dfOut = dfPersonal.select('id','email','country')
    logger.debug('created Out Dataframe with removed columns from Personal DataFrame')

    dfOut =  customFilter(dfOut,{'country':countryFilter})
    logger.debug(f'filtered columns on Out DataFrame with countryFilter:{countryFilter}')

    dfOut = dfOut.join(dfAccounts,on=dfPersonal.id==dfAccounts.id,how='inner')
    logger.debug('joined with Accounts DataFrame')

    dfOut = dfOut.drop(dfAccounts.id)
    logger.debug('removed duplicated join column from Out Dataframe')

    dfOut = customRename(dfOut,json_loads(config["LOGIC"]["renameMap"]))
    logger.debug(f'renamed columns on Out DataFrame with renameMap:{json_loads(config["LOGIC"]["renameMap"])}')

    logger.debug('dfOut count:'+str(dfOut.count()))

    dfOut.write.csv(config[SPARK][outputFolderName],header=True,mode='overwrite')
    logger.info(f'saved Out Dataframe to folder:{config[SPARK][outputFolderName]}')

if __name__ == '__main__':
    main()