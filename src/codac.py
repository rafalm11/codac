#codac app main module

from argparse import ArgumentParser
from sys import argv
from sys import stdout
from logging.handlers import RotatingFileHandler
import logging

from pyspark.sql import SparkSession

def getParameters(params: list):
    #function takes parameters list to ease testing
    logger.debug('getParameters started')
    parser = ArgumentParser(
        prog='codac.py',
        description='joins customer personal data with its accounts')
    #required parameters
    parser.add_argument('-p', '-personalFile', dest='personalFileName', type=str, help="consumer's personal data file (csv)", required=True, metavar='<fileName>')
    parser.add_argument('-a', '-accountsFile', dest='accountsFileName', type=str, help="consumer's accounts data file (csv)", required=True, metavar='<fileName>')

    #optional paarmeters
    parser.add_argument('-o', '-outputFolder', dest='outputFolderName', type=str, help="output data folder (will be overwritten. default:client_data)", 
        required=False, metavar='<folderName>', default='client_data')
    parser.add_argument('-c', '-countryFilter', dest='countryFilter', type=str, 
        help='list of countries to filter by (default: Netherlands)', required=False, default ='Netherlands', metavar='<countryName>', nargs='*')
    parser.add_argument('-s', '-sparkUrl', dest='sparkUrl', type=str, 
        help='spark instance url (default: local)', required=False, default ='local', metavar='<url>')
    
    args = parser.parse_args(params)
    return args.personalFileName, args.accountsFileName, args.outputFolderName, args.countryFilter, args.sparkUrl

def loggerInit(loggerName: str):    
    loggerI = logging.getLogger(loggerName)
    loggerI.setLevel(level=logging.DEBUG)    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handlerFile = logging.handlers.RotatingFileHandler('logs\\test.log',maxBytes=10000,backupCount=5)
    handlerFile.setFormatter(formatter)
    loggerI.addHandler(handlerFile)
    handlerConsole = logging.StreamHandler(stdout)
    handlerConsole.setFormatter(formatter)
    loggerI.addHandler(handlerConsole)
    loggerI.debug('loggerInit finished')
    return loggerI
    
logger = loggerInit(__name__)

from pyspark.sql import DataFrame
from typing import Dict, List


def customFilter(df: DataFrame, filterMap: Dict[str, List[str]]) -> DataFrame:
    logger.debug('customFilter started. filter:'+str(filterMap))
    for column,valuesList in filterMap.items():
        df = df.filter(df[column].isin(valuesList))
        logger.debug('filter by column:'+column+' with values:'+str(valuesList))
    return df

def customRename(df: DataFrame, renameMap: Dict[str,str]) -> DataFrame:
    logger.debug('customRename started. renameMap:'+str(renameMap))
    df = df.withColumnsRenamed(renameMap)
    return df



def main():
    logger.info('main started')
    personalFileName, accountsFileName, outputFolderName, countryFilter, sparkUrl  = getParameters(argv[1:])
    logger.debug('parameters. p:'+personalFileName+' a:'+accountsFileName+' o:'+outputFolderName+' c:'+str(countryFilter)+' u:'+sparkUrl)
    
    sparkSession = SparkSession.builder.appName('codac').master(sparkUrl).getOrCreate()
    logger.debug('spark session created')

    dfPersonal = sparkSession.read.csv(path=personalFileName,header=True)
    logger.info('opened Personal DataFrame from file:'+personalFileName)

    dfAccounts = sparkSession.read.csv(path=accountsFileName,header=True)
    logger.info('opened Accounts DataFrame from file:'+accountsFileName)

    dfOut = dfPersonal.select('id','email','country')
    logger.debug('created Out Dataframe with removed columns from Personal DataFrame')

    dfOut =  customFilter(dfOut,{'country':countryFilter}) #dfOut.filter(dfPersonal.country.isin(countryFilter))
    logger.debug('filtered columns on Out DataFrame')

    dfOut = dfOut.join(dfAccounts,on=dfPersonal.id==dfAccounts.id,how='inner')
    logger.debug('joined with Accounts DataFrame')

    dfOut = dfOut.drop(dfAccounts.id)
    logger.debug('removed duplicated join column from Out Dataframe')

    dfOut = customRename(dfOut,{'id':'client_identifier','btc_a':'bitcoin_address','cc_t':'credit_card_type'}) #dfOut.withColumnsRenamed({'id':'client_identifier','btc_a':'bitcoin_address','cc_t':'credit_card_type'})
    logger.debug('renamed columns on Out DataFrame')

    logger.debug('dfOut count:'+str(dfOut.count()))

    dfOut.write.csv(outputFolderName,header=True,mode='overwrite')
    logger.info('saved Out Dataframe to folder:'+outputFolderName)

if __name__ == '__main__':
    main()