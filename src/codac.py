#codac app main module

from argparse import ArgumentParser
from sys import argv

def getParameters(params):
    #function takes parameters list to ease testing
    parser = ArgumentParser(
        prog='codac.py',
        description='joins customer personal data with its accounts')
    parser.add_argument('-p', '-personalFile', dest='personalFileName', type=str, help="consumer's personal data file (csv)", required=True, metavar='<fileName>')
    parser.add_argument('-a', '-accountsFile', dest='accountsFileName', type=str, help="consumer's accounts data file (csv)", required=True, metavar='<fileName>')
    parser.add_argument('-o', '-outputFolder', dest='outputFolderName', type=str, help="output data folder (will be overwritten. default:client_data)", 
        required=False, metavar='<folderName>', default='client_data')
    parser.add_argument('-c', '-countryFilter', dest='countryFilter', type=str, 
        help='list of countries to filter by (default: Netherlands)', required=False, default ='Netherlands', metavar='<countryName>', nargs='*')
    parser.add_argument('-s', '-sparkUrl', dest='sparkUrl', type=str, 
        help='spark instance url (default: local)', required=False, default ='local', metavar='<url>')
    
    args = parser.parse_args(params)
    return args.personalFileName, args.accountsFileName, args.outputFolderName, args.countryFilter, args.sparkUrl

def main():
    personalFileName, accountsFileName, outputFolderName, countryFilter, sparkUrl  = getParameters(argv[1:])
    print (personalFileName)
    print (accountsFileName)
    print (outputFolderName)
    print (countryFilter)
    print (sparkUrl)

if __name__ == '__main__':
    main()