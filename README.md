# CodacJoiner

## description
This application is created for very small company that helps marketing actions.
Application purpose is to join two files of data in a single one containing data necessary for marketing campaign.


## details
Application requires two input files:
- consumers personal data with below structure (headers required):
    |id|first_name|last_name|email|country|
- accounts data with below struncture (headers required):
    |id|btc_a|cc_t|cc_n|
- and produces output file:
    |client_identifier(sourced from id)|email|country|bitcoin_address(sourced from btc_a)|credit_card_type(sourced from cc_t)|cc_n|

## use synatax
'''
usage: codac.py [-h] -p <fileName> -a <fileName> [-o <folderName>]
                [-c [<countryName> [<countryName> ...]]] [-s <url>]

joins customer personal data with its accounts

optional arguments:
  -h, --help            show this help message and exit
  -p <fileName>, -personalFile <fileName>
                        consumer's personal data file (csv)
  -a <fileName>, -accountsFile <fileName>
                        consumer's accounts data file (csv)
  -o <folderName>, -outputFolder <folderName>
                        output data folder (will be overwritten.
                        default:client_data)
  -c [<countryName> [<countryName> ...]], -countryFilter [<countryName> [<countryName> ...]]
                        list of countries to filter by (default: Netherlands)
  -s <url>, -sparkUrl <url>
                        spark instance url (default: local)
'''
## use examples
> python src/codac.py -p './client_input/dataset_one.csv' -a './client_input/dataset_two.csv'
> python src/codac.py -p './client_input/dataset_one.csv' -a './client_input/dataset_two.csv' -c 'United Kingdom' Netherlands

