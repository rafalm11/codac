import pytest
from src.codac import getParameters

def test_parameters_3_countries():
    assert getParameters(['-pf','aaa.csv','-af','bbb.csv','-c','c1','c2','c3'])==('aaa.csv','bbb.csv',['c1','c2','c3'])

def test_parameters_none_countries():
    assert getParameters(['-pf','aaa.csv','-af','bbb.csv'])==('aaa.csv','bbb.csv','Netherlands')

def test_parameters_displaced_countries():
    assert getParameters(['-c','c1','c2','c3','-pf','aaa.csv','-af','bbb.csv'])==('aaa.csv','bbb.csv',['c1','c2','c3'])

def test_missing_file_names():
    with pytest.raises(SystemExit) as info: 
        getParameters(['-c','c1'])
