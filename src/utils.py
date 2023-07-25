import logging
from argparse import ArgumentParser
from configparser import ConfigParser
from logging.handlers import RotatingFileHandler
from sys import stdout
from typing import Tuple


def get_config(fileName: str) -> ConfigParser:
    """Return config object that holds parameters defined in .ini file."""
    config = ConfigParser()
    config.read(fileName)
    return config


def get_parameters(params: list) -> Tuple[str]:
    """Get calling parameters."""

    config = get_config("codac.ini")

    parser = ArgumentParser(
        prog="codac.py", description="joins customer personal data with its accounts."
    )
    # required parameters
    parser.add_argument(
        "-p",
        "-personalFile",
        dest="personalFileName",
        type=str,
        help="consumer's personal data file (csv)",
        required=True,
        metavar="<fileName>",
    )
    parser.add_argument(
        "-a",
        "-accountsFile",
        dest="accountsFileName",
        type=str,
        help="consumer's accounts data file (csv)",
        required=True,
        metavar="<fileName>",
    )

    # optional parameters
    parser.add_argument(
        "-c",
        "-countryFilter",
        dest="countryFilter",
        type=str,
        help="list of countries to filter by (default: specified in utils.ini)",
        required=False,
        default=config["DEFAULT"]["countryFilter"],
        metavar="<countryName>",
        nargs="*",
    )

    args = parser.parse_args(params)
    return args.personalFileName, args.accountsFileName, args.countryFilter


def logger_init(loggerName: str) -> logging.Logger:
    """Initialize logger object."""
    config = get_config("codac.ini")

    loggerI = logging.getLogger(loggerName)
    loggerI.setLevel(level=logging.DEBUG)
    formatter = logging.Formatter(config["LOGS"]["logFormat"])
    handlerFile = logging.handlers.RotatingFileHandler(
        config["LOGS"]["logsFile"],
        maxBytes=int(config["LOGS"]["maxBytes"]),
        backupCount=int(config["LOGS"]["backupCount"]),
    )
    handlerFile.setFormatter(formatter)
    loggerI.addHandler(handlerFile)
    handlerConsole = logging.StreamHandler(stdout)
    handlerConsole.setFormatter(formatter)
    loggerI.addHandler(handlerConsole)
    loggerI.debug("loggerInit finished")
    return loggerI
