import pytest
from src.codac import getParameters

def test_parameters_output_file():
    assert getParameters(['-p','aaa.csv','-a','bbb.csv','-o','testOutput'])==('aaa.csv','bbb.csv','testOutput','Netherlands','local')

def test_parameters_3_countries():
    assert getParameters(['-p','aaa.csv','-a','bbb.csv','-c','c1','c2','c3'])==('aaa.csv','bbb.csv','client_data',['c1','c2','c3'],'local')

def test_parameters_url():
    assert getParameters(['-p','aaa.csv','-a','bbb.csv','-s','test.spark.url'])==('aaa.csv','bbb.csv','client_data','Netherlands','test.spark.url')

def test_parameters_default_values():
    assert getParameters(['-p','aaa.csv','-a','bbb.csv'])==('aaa.csv','bbb.csv','client_data','Netherlands','local')

def test_parameters_displaced_countries():
    assert getParameters(['-c','c1','c2','c3','-p','aaa.csv','-a','bbb.csv'])==('aaa.csv','bbb.csv', 'client_data', ['c1','c2','c3'],'local')

def test_missing_file_names():
    with pytest.raises(SystemExit) as info: 
        getParameters(['-c','c1'])
