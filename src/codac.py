#codac app main module

from argparse import ArgumentParser
from sys import argv

def getParameters(params):
    #function takes parameters list to ease testing
    parser = ArgumentParser(
        prog='codac.py',
        description='joins customer personal data with its accounts')
    parser.add_argument('-pf', '-personalFile', dest='personalFileName', type=str, help="consumer's personal data file (csv)", required=True, metavar='fileName')
    parser.add_argument('-af', '-accountsFile', dest='accountsFileName', type=str, help="consumer's accounts data file (csv)", required=True, metavar='fileName')
    parser.add_argument('-c', '-countryFilter', dest='countryFilter', type=str, 
        help='list of countries to filter by (default: Netherlands)', required=False, default ='Netherlands', metavar='countryName', nargs='*')
    args = parser.parse_args(params)
    return args.personalFileName, args.accountsFileName, args.countryFilter

def main():
    personalFileName, accountsFileName, countryFilter  = getParameters(argv[1:])
    print (personalFileName)
    print (accountsFileName)
    print (countryFilter)

if __name__ == '__main__':
    main()