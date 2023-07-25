from json import loads as json_loads
import pytest
from utils import get_parameters, logger_init, get_config

# test get_arameters


def test_parameters_3_countries():
    assert get_parameters(
        ["-p", "aaa.csv", "-a", "bbb.csv", "-c", "c1", "c2", "c3"]
    ) == ("aaa.csv", "bbb.csv", ["c1", "c2", "c3"])


def test_parameters_default_values():
    assert get_parameters(["-p", "aaa.csv", "-a", "bbb.csv"]) == (
        "aaa.csv",
        "bbb.csv",
        "Netherlands",
    )


def test_parameters_displaced_countries():
    assert get_parameters(
        ["-c", "c1", "c2", "c3", "-p", "aaa.csv", "-a", "bbb.csv"]
    ) == ("aaa.csv", "bbb.csv", ["c1", "c2", "c3"])


def test_missing_file_names():
    with pytest.raises(SystemExit) as info:
        get_parameters(["-c", "c1"])


# test logging


def test_log(caplog):
    get_parameters(["-p", "aaa.csv", "-a", "bbb.csv"])
    assert "get_parameters started" in caplog.text


# test config


def test_config():
    config = get_config("codac.ini")
    assert json_loads(config["LOGIC"]["renameMap"]) == {
        "id": "client_identifier",
        "btc_a": "bitcoin_address",
        "cc_t": "credit_card_type",
    }
