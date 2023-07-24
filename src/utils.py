from argparse import ArgumentParser
from logging.handlers import RotatingFileHandler
import logging
from sys import stdout
from typing import Tuple

def getParameters(params: list) -> Tuple[str]:
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

def loggerInit(loggerName: str) -> logging.Logger:    
    loggerI = logging.getLogger(loggerName)
    loggerI.setLevel(level=logging.DEBUG)    
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handlerFile = logging.handlers.RotatingFileHandler('logs/test.log',maxBytes=10000,backupCount=5)
    handlerFile.setFormatter(formatter)
    loggerI.addHandler(handlerFile)
    handlerConsole = logging.StreamHandler(stdout)
    handlerConsole.setFormatter(formatter)
    loggerI.addHandler(handlerConsole)
    loggerI.debug('loggerInit finished')
    return loggerI

logger = loggerInit(__name__)