from argparse import ArgumentParser
from configparser import ConfigParser
from logging.handlers import RotatingFileHandler
import logging
from sys import stdout
from typing import Tuple

def getConfig(fileName: str) -> ConfigParser:
    config = ConfigParser()
    config.read(fileName)
    return config

def getParameters(params: list) -> Tuple[str]:
    #function takes parameters list to ease testing
    logger.debug('getParameters started')
    
    config = getConfig('codac.ini')

    parser = ArgumentParser(
        prog='codac.py',
        description='joins customer personal data with its accounts')
    #required parameters
    parser.add_argument('-p', '-personalFile', dest='personalFileName', type=str, help="consumer's personal data file (csv)", required=True, metavar='<fileName>')
    parser.add_argument('-a', '-accountsFile', dest='accountsFileName', type=str, help="consumer's accounts data file (csv)", required=True, metavar='<fileName>')

    #optional parameters
    parser.add_argument('-c', '-countryFilter', dest='countryFilter', type=str, 
        help='list of countries to filter by (default: specified in utils.ini)', required=False, default =config['DEFAULT']['countryFilter'], metavar='<countryName>', nargs='*')
    
    args = parser.parse_args(params)
    return args.personalFileName, args.accountsFileName, args.countryFilter

def loggerInit(loggerName: str) -> logging.Logger:    
    config = getConfig('codac.ini')

    loggerI = logging.getLogger(loggerName)
    loggerI.setLevel(level=logging.DEBUG)    
    formatter = logging.Formatter(config['LOGS']['logFormat'])
    handlerFile = logging.handlers.RotatingFileHandler(config['LOGS']['logsFile'],maxBytes=int(config['LOGS']['maxBytes']),backupCount=int(config['LOGS']['backupCount']))
    handlerFile.setFormatter(formatter)
    loggerI.addHandler(handlerFile)
    handlerConsole = logging.StreamHandler(stdout)
    handlerConsole.setFormatter(formatter)
    loggerI.addHandler(handlerConsole)
    loggerI.debug('loggerInit finished')
    return loggerI

logger = loggerInit(__name__)