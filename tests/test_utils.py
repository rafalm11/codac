from json import loads as json_loads
import pytest
from utils import getParameters, loggerInit, getConfig

# test getParameters


def test_parameters_3_countries():
    assert getParameters(
        ["-p", "aaa.csv", "-a", "bbb.csv", "-c", "c1", "c2", "c3"]
    ) == ("aaa.csv", "bbb.csv", ["c1", "c2", "c3"])


def test_parameters_default_values():
    assert getParameters(["-p", "aaa.csv", "-a", "bbb.csv"]) == (
        "aaa.csv",
        "bbb.csv",
        "Netherlands",
    )


def test_parameters_displaced_countries():
    assert getParameters(
        ["-c", "c1", "c2", "c3", "-p", "aaa.csv", "-a", "bbb.csv"]
    ) == ("aaa.csv", "bbb.csv", ["c1", "c2", "c3"])


def test_missing_file_names():
    with pytest.raises(SystemExit) as info:
        getParameters(["-c", "c1"])


# test logging


def test_log(caplog):
    getParameters(["-p", "aaa.csv", "-a", "bbb.csv"])
    assert "getParameters started" in caplog.text


# test config


def test_config():
    config = getConfig("codac.ini")
    assert json_loads(config["LOGIC"]["renameMap"]) == {
        "id": "client_identifier",
        "btc_a": "bitcoin_address",
        "cc_t": "credit_card_type",
    }
