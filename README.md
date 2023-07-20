# CodacJoiner


This application is created for very small company that helps marketing actions.
Application purpose is to join two files of data in a single one containing data necessary for marketing campaign.


details:

Application requires two input files:
1. consumers personal data with below structure (headers required):
    id|first_name|last_name|email|country
2. accounts data with below struncture (headers required):
    id|btc_a|cc_t|cc_n

and produces output file:
    client_identifier(sourced from id)|email|country|bitcoin_address(sourced from btc_a)|credit_card_type(sourced from cc_t)|cc_n


usage: codac.py [-h] -pf fileName -af fileName
             [-c [countryName [countryName ...]]]

joins customer personal data with its accounts

optional arguments:
  -h, --help            show this help message and exit
  -pf fileName, -personalFile fileName
                        consumer's personal data file (csv)
  -af fileName, -accountsFile fileName
                        consumer's accounts data file (csv)
  -c [countryName [countryName ...]], -countryFilter [countryName [countryName ...]]
                        list of countries to filter by (default: Netherland)

use examples:
python codac.py -pf dataset_one.csv -af dataset_two.csv -c 'United Kingdom' Netherlands
python codac.py -pf dataset_one.csv -af dataset_two.csv
