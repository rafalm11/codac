# CodacJoiner

## description
This application is created for very small company that helps marketing actions.
Application purpose is to join two files of data in a single one containing data necessary for marketing campaign.


## details
Application requires two input files:
- consumers personal data with below structure (headers required):
    |id|first_name|last_name|email|country|
    |---|---|---|---|---|
- accounts data with below struncture (headers required):
    |id|btc_a|cc_t|cc_n|
    |---|---|---|---|
- and joins them together creating output file:
    |client_identifier|email|country|bitcoin_address|credit_card_typels|cc_n|
    |(sourced from id)|   |   |(sourced from btc_a)|(sourced from cc_t)|   |
    |---|---|---|---|---|---|

## use synatax
```
usage: codac.py [-h] -p <fileName> -a <fileName>
                [-c [<countryName> [<countryName> ...]]]

joins customer personal data with its accounts

optional arguments:
  -h, --help            show this help message and exit
  -p <fileName>, -personalFile <fileName>
                        consumer's personal data file (csv)
  -a <fileName>, -accountsFile <fileName>
                        consumer's accounts data file (csv)
  -c [<countryName> [<countryName> ...]], -countryFilter [<countryName> [<countryName> ...]]
                        list of countries to filter by (default: specified in
                        utils.ini)
```

## use examples
> python src/codac.py -p './client_input/dataset_one.csv' -a './client_input/dataset_two.csv'

> python src/codac.py -p './client_input/dataset_one.csv' -a './client_input/dataset_two.csv' -c 'United Kingdom' Netherlands

