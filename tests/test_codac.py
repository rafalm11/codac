import pytest

#test getParameters
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


# test logging
def test_log(caplog):
    getParameters(['-p','aaa.csv','-a','bbb.csv'])
    assert 'getParameters started' in caplog.text


# test pyspark 
import chispa
from pyspark.sql import SparkSession
from src.codac import customFilter, customRename

@pytest.fixture
def spark():
    return SparkSession.builder.master('local').getOrCreate()

@pytest.fixture
def sourceData():
    return[('name1','surname1','country1'),
            ('name2','surname2','country1'),
            ('name3','surname3','country3'),
            ('name4','surname4','country4')]
@pytest.fixture
def sourceColumns():
    return['name','surname','country']

@pytest.fixture
def dfSource(spark,sourceData,sourceColumns):
    return spark.createDataFrame(sourceData,sourceColumns)

def test_customFilter_1country(spark,dfSource):
    dfExpected = spark.createDataFrame([('name1','surname1','country1'),
                                ('name2','surname2','country1'),                                
                                ('name4','surname4','country4')],['name','surname','country'])
    chispa.assert_df_equality(customFilter(dfSource,{'country':['country1','country4']}),dfExpected)

def test_custom_filter_2countries(spark,dfSource):
    dfExpected = spark.createDataFrame([('name1','surname1','country1'),
                                ('name2','surname2','country1')],['name','surname','country'])
    chispa.assert_df_equality(customFilter(dfSource,{'country':'country1'}),dfExpected)

def test_customRename(spark,sourceData,sourceColumns, dfSource):
    dfExpected = spark.createDataFrame(sourceData,['firstname','lastname','country'])
    chispa.assert_df_equality(customRename(df=dfSource, renameMap={'name':'firstname','surname':'lastname'}),dfExpected)