__author__ = "Justin Strong"
__email__ = "justin.strong@intel.com"
__description__ = "This script stages ICap Company data in the GSMDW database on sql1717-fm1-in.amr.corp.intel.com,3181"
__schedule__ = "Weekly on Sunday at 12:03 PM PST"

import os
import sys; sys.path.append(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))  # add current file's parent directory to path
import pandas as pd
from datetime import datetime
from Helper_Functions import querySalesforce, uploadDFtoSQL
from Logging import log

# remove the current file's parent directory from sys.path since it was only needed for imports above
try:
    sys.path.remove(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir))
except ValueError:  # Already removed
    pass


if __name__ == "__main__":
    # Proxies needed for code to run inside of VPN
    os.environ['http_proxy'] = 'http://proxy-chain.intel.com:911'
    os.environ['https_proxy'] = 'https://proxy-chain.intel.com:912'

    # initialize variables
    project_name = 'ICap'
    data_area = 'Salesforce Accounts'
    table = 'dbo.ICAPCompany'

    # Extract data from Salesforce
    # df = querySalesforce("select name,owner.name,ICAP_Public_Private__c,ICAP_Disclosure_Level__c,website,BillingStreet, BillingCity, BillingState, BillingPostalCode, BillingCountry, ICAP_Investment_Status__c from account where ICAP_Investment_Status__c='Active' order by name asc")
    df = querySalesforce("select name, owner.name from account where ICAP_Investment_Status__c='Active' order by name asc")

    # Transform data
    df['ownerName'] = df.Owner.apply(pd.Series)['Name']  # Extract ICAP owner name into new column

    df.drop(df.columns.difference(['Name', 'ownerName']), axis=1, inplace=True)  # Remove other columns

    df['LastUpdated'] = datetime.now()  # Add upload datetime

    # Load data to SQL database
    insert_succeeded, error_msg = uploadDFtoSQL(table=table, data=df, truncate=True)
    log(insert_succeeded, project_name=project_name, data_area=data_area, row_count=df.shape[0], error_msg=error_msg)
    if insert_succeeded:
        print('Successfully inserted {0} rows into {1}'.format(df.shape[0], table))
    else:
        print(error_msg)
