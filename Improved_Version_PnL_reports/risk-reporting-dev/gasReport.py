import pandas as pd
import requests
import os
from datetime import datetime, timezone
from time import sleep
import psycopg2
from credentials import accountERC20, accountETH, accountSubsidy, accountCold, accountCustody, accountBEP20, accountSubsidyBEP20, dwh_user, dwh_pass

# Inputs
start = datetime(2021, 7, 19, 0, 0, 0, tzinfo = timezone.utc)
end = datetime(2021, 7, 25, 23, 59, 59, tzinfo = timezone.utc)
starttimestamp = int(start.timestamp())
endtimestamp = int(end.timestamp())

converter = 0.000000001
valueConverter = 0.000000000000000001

# Define functions
def run_dwh_sql(sql_query):
    conn = psycopg2.connect(host='135.181.61.116', user=dwh_user, password=dwh_pass, dbname='postgres',port='5432')
    data = pd.read_sql(sql_query, conn)
    conn.close()
    return data

def transactions(start, end):
    sql_query = f"""
    select vtmtl.id                as transaction_id,
           vtmtl.transaction_hash  as transaction_hash,
           vtmtl."user"            as user_id,
           mvtmcpl.name            as currency_provider_name,
           mvtmppl.name            as payment_provider_name,
           mvamcl.tag              as currency_tag,
           mvamcl.name             as currency_name,
           vtmtl.transferred_funds as value,
           vtmtl.usd_value,
           vtmtl.transaction_fee   as fee_value,
           vtmtl.status,
           vtmtl.__update_datetime as updated_at,
           vtmtl.type              as transaction_type
    from view_transaction_manager_transaction_latest vtmtl
             left join materialized_view_transaction_manager_currency_binding_latest mvtmcbl
                       on vtmtl.currency_binding_id = mvtmcbl.id
             left join materialized_view_transaction_manager_currency_provider_latest mvtmcpl
                       on mvtmcbl.currency_provider_id = mvtmcpl.id
             left join materialized_view_transaction_manager_payment_provider_latest mvtmppl
                       on mvtmcpl.provider_id = mvtmppl.id
             left join materialized_view_asset_manager_currency_latest mvamcl
                       on mvtmcbl.currency = mvamcl.id
    where vtmtl.__update_datetime >= '{start}' and vtmtl.__update_datetime <= '{end}'
    and vtmtl.status = 'CONFIRMED' and mvtmppl.name in ('ERC20', 'ETH', 'BSC_TOKEN');
    """
    return run_dwh_sql(sql_query)

# Running main part
if __name__ == '__main__':
    # ERC20 fees
    startblock = int(requests.get(f'https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={starttimestamp}&closest=before').json()['result'])
    sleep(5)
    endblock = int(requests.get(f'https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={endtimestamp}&closest=after').json()['result'])
    sleep(5)
    transactionsERC20 = requests.get(f'https://api.etherscan.io/api?module=account&action=txlist&address={accountERC20}&sort=asc&startblock={startblock}&endblock={endblock}').json()
    sleep(5)
    allTransactionsERC20 = list()
    for i in range(len(transactionsERC20['result'])):
        if transactionsERC20['result'][i]['from'] == accountERC20:
            fee = float(transactionsERC20['result'][i]['gasPrice']) * converter * float(transactionsERC20['result'][i]['gasUsed']) * converter
            allTransactionsERC20.append(fee)
    totalERC20 = sum(allTransactionsERC20)

    transactionCostAvgERC20 = sum(allTransactionsERC20)/len(allTransactionsERC20)
    transactionsNumberERC20 = len(transactionsERC20['result'])

    # ETH fees
    startblock = int(requests.get(f'https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={starttimestamp}&closest=before').json()['result'])
    sleep(5)
    endblock = int(requests.get(f'https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={endtimestamp}&closest=after').json()['result'])
    sleep(5)
    transactionsETH = requests.get(f'https://api.etherscan.io/api?module=account&action=txlist&address={accountETH}&sort=asc&startblock={startblock}&endblock={endblock}').json()
    sleep(5)
    allTransactionsETH = list()
    for i in range(len(transactionsETH['result'])):
        if transactionsETH['result'][i]['from'] == accountETH:
            fee = float(transactionsETH['result'][i]['gasPrice']) * converter * float(transactionsETH['result'][i]['gasUsed']) * converter
            allTransactionsETH.append(fee)
    totalETH = sum(allTransactionsETH)

    transactionCostAvgETH = sum(allTransactionsETH)/len(allTransactionsETH)
    transactionsNumberETH = len(transactionsETH['result'])

    # Subsidy fees
    startblock = int(requests.get(f'https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={starttimestamp}&closest=before').json()['result'])
    sleep(5)
    endblock = int(requests.get(f'https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={endtimestamp}&closest=after').json()['result'])
    sleep(5)
    transactionsSubsidy = requests.get(f'https://api.etherscan.io/api?module=account&action=txlist&address={accountSubsidy}&sort=asc&startblock={startblock}&endblock={endblock}').json()
    sleep(5)
    allTransactionsSubsidy = list()
    for i in range(len(transactionsSubsidy['result'])):
        if transactionsSubsidy['result'][i]['from'] == accountSubsidy:
            fee = float(transactionsSubsidy['result'][i]['gasPrice']) * converter * float(transactionsSubsidy['result'][i]['gasUsed']) * converter + float(transactionsSubsidy['result'][i]['value']) * valueConverter
            allTransactionsSubsidy.append(fee)
        else:
            fee = float(transactionsSubsidy['result'][i]['gasPrice']) * converter * float(transactionsSubsidy['result'][i]['gasUsed']) * converter 
            allTransactionsSubsidy.append(fee)
    totalSubsidy = sum(allTransactionsSubsidy)

    transactionCostAvgSubsidy = sum(allTransactionsSubsidy)/len(allTransactionsSubsidy)
    transactionsNumberSubsidy = len(transactionsSubsidy['result'])

    # Cold fees
    startblock = int(requests.get(f'https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={starttimestamp}&closest=before').json()['result'])
    sleep(5)
    endblock = int(requests.get(f'https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={endtimestamp}&closest=after').json()['result'])
    sleep(5)
    transactionsCold = requests.get(f'https://api.etherscan.io/api?module=account&action=txlist&address={accountCold}&sort=asc&startblock={startblock}&endblock={endblock}').json()
    sleep(5)
    allTransactionsCold = list()
    for i in range(len(transactionsCold['result'])):
        if transactionsCold['result'][i]['from'] == accountCold:
            fee = float(transactionsCold['result'][i]['gasPrice']) * converter * float(transactionsCold['result'][i]['gasUsed']) * converter
            allTransactionsCold.append(fee)
    totalCold = sum(allTransactionsCold)

    try:
        transactionCostAvgCold = sum(allTransactionsCold)/len(allTransactionsCold)
    except:
        transactionCostAvgCold = 0
    transactionsNumberCold = len(transactionsCold['result'])

    # Custody fees
    startblock = int(requests.get(f'https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={starttimestamp}&closest=before').json()['result'])
    sleep(5)
    endblock = int(requests.get(f'https://api.etherscan.io/api?module=block&action=getblocknobytime&timestamp={endtimestamp}&closest=after').json()['result'])
    sleep(5)
    transactionsCustody = requests.get(f'https://api.etherscan.io/api?module=account&action=txlist&address={accountCustody}&sort=asc&startblock={startblock}&endblock={endblock}').json()
    sleep(5)
    allTransactionsCustody = list()
    for i in range(len(transactionsCustody['result'])):
        if transactionsCustody['result'][i]['from'] == accountCustody:
            fee = float(transactionsCustody['result'][i]['gasPrice']) * converter * float(transactionsCustody['result'][i]['gasUsed']) * converter
            allTransactionsCustody.append(fee)
    totalCustody = sum(allTransactionsCustody)

    try:
        transactionCostAvgCustody = sum(allTransactionsCustody)/len(allTransactionsCustody)
    except:
        transactionCostAvgCustody = 0
    transactionsNumberCustody = len(transactionsCustody['result'])

    # BSC Hot fees
    startblock = int(requests.get(f'https://api.bscscan.com/api?module=block&action=getblocknobytime&timestamp={starttimestamp}&closest=before').json()['result'])
    sleep(5)
    endblock = int(requests.get(f'https://api.bscscan.com/api?module=block&action=getblocknobytime&timestamp={endtimestamp}&closest=after').json()['result'])
    sleep(5)
    transactionsBEP20 = requests.get(f'https://api.bscscan.com/api?module=account&action=txlist&address={accountBEP20}&sort=asc&startblock={startblock}&endblock={endblock}').json()
    sleep(5)
    allTransactionsBEP20 = list()
    for i in range(len(transactionsBEP20['result'])):
        if transactionsBEP20['result'][i]['from'].lower() == accountBEP20.lower():
            fee = float(transactionsBEP20['result'][i]['gasPrice']) * converter * float(transactionsBEP20['result'][i]['gasUsed']) * converter
            allTransactionsBEP20.append(fee)
    totalBEP20 = sum(allTransactionsBEP20)

    transactionCostAvgBEP20 = sum(allTransactionsBEP20)/len(allTransactionsBEP20)
    transactionsNumberBEP20 = len(transactionsBEP20['result'])

    # BSC Subsidy fees
    startblock = int(requests.get(f'https://api.bscscan.com/api?module=block&action=getblocknobytime&timestamp={starttimestamp}&closest=before').json()['result'])
    sleep(5)
    endblock = int(requests.get(f'https://api.bscscan.com/api?module=block&action=getblocknobytime&timestamp={endtimestamp}&closest=after').json()['result'])
    sleep(5)
    transactionsSubsidyBEP20 = requests.get(f'https://api.bscscan.com/api?module=account&action=txlist&address={accountSubsidyBEP20}&sort=asc&startblock={startblock}&endblock={endblock}').json()
    sleep(5)
    allTransactionsSubsidyBEP20 = list()
    for i in range(len(transactionsSubsidyBEP20['result'])):
        if transactionsSubsidyBEP20['result'][i]['from'] == accountSubsidyBEP20:
            fee = float(transactionsSubsidyBEP20['result'][i]['gasPrice']) * converter * float(transactionsSubsidyBEP20['result'][i]['gasUsed']) * converter + float(transactionsSubsidyBEP20['result'][i]['value']) * valueConverter
            allTransactionsSubsidyBEP20.append(fee)
        else:
            fee = float(transactionsSubsidyBEP20['result'][i]['gasPrice']) * converter * float(transactionsSubsidyBEP20['result'][i]['gasUsed']) * converter 
            allTransactionsSubsidyBEP20.append(fee)
    totalSubsidyBEP20 = sum(allTransactionsSubsidyBEP20)

    transactionCostAvgSubsidyBEP20 = sum(allTransactionsSubsidyBEP20)/len(allTransactionsSubsidyBEP20)
    transactionsNumberSubsidyBEP20 = len(transactionsSubsidyBEP20['result'])

    # Getting ETH price from Binance
    endMil = int(str(endtimestamp)+'000')
    priceETH = float(requests.get(f"https://api.binance.com/api/v3/klines?symbol=ETHUSDT&interval=1h&startTime={endMil}&limit=5").json()[0][1])
    priceBNB = float(requests.get(f"https://api.binance.com/api/v3/klines?symbol=BNBUSDT&interval=1h&startTime={endMil}&limit=5").json()[0][1])

    # Getting transactions data
    transactData = transactions(start, end)
    # ERC20 data
    withdrawalAvgERC20 = transactData[(transactData.transaction_type=='WITHDRAWAL')&(transactData.payment_provider_name=='ERC20')].usd_value.mean()
    withdrawalMedERC20 = transactData[(transactData.transaction_type=='WITHDRAWAL')&(transactData.payment_provider_name=='ERC20')].usd_value.median()
    withdrawelersERC20 = len(transactData[(transactData.transaction_type=='WITHDRAWAL')&(transactData.payment_provider_name=='ERC20')].user_id.unique())
    # ETH data
    withdrawalAvgETH = transactData[(transactData.transaction_type=='WITHDRAWAL')&(transactData.payment_provider_name=='ETH')].usd_value.mean()
    withdrawalMedETH = transactData[(transactData.transaction_type=='WITHDRAWAL')&(transactData.payment_provider_name=='ETH')].usd_value.median()
    withdrawelersETH = len(transactData[(transactData.transaction_type=='WITHDRAWAL')&(transactData.payment_provider_name=='ETH')].user_id.unique())
    # BSC data
    withdrawalAvgBSC = transactData[(transactData.transaction_type=='WITHDRAWAL')&(transactData.payment_provider_name=='BSC_TOKEN')].usd_value.mean()
    withdrawalMedBSC = transactData[(transactData.transaction_type=='WITHDRAWAL')&(transactData.payment_provider_name=='BSC_TOKEN')].usd_value.median()
    withdrawelersBSC = len(transactData[(transactData.transaction_type=='WITHDRAWAL')&(transactData.payment_provider_name=='BSC_TOKEN')].user_id.unique())
   

    # Final dataframe
    finalTable = pd.DataFrame({
        'Account': ['ERC20', 'ETH', 'Subsidy ERC20', 'Custody ERC20', 'Cold ERC20', 'ETH Price', 'Subtotal ETH, $:', '', 
                    'BEP20', 'Subsidy BEP20', 'BNB Price', 'Subtotal BSC, $:', '', 'Total, $'],
        'Gas Amount': [round(totalERC20, 2), round(totalETH, 2), round(totalSubsidy, 2), round(totalCustody, 2), 
                       round(totalCold, 2), priceETH, round((totalERC20 + totalETH + totalSubsidy + totalCustody + totalCold) * priceETH, 2), 
                       '', round(totalBEP20, 5), round(totalSubsidyBEP20, 5), priceBNB, round((totalBEP20 + totalSubsidyBEP20) * priceBNB, 2), 
                       '', round(((totalERC20 + totalETH + totalSubsidy + totalCustody + totalCold) * priceETH) + 
                        ((totalBEP20 + totalSubsidyBEP20) * priceBNB), 2)], 
        '# of Transactions': [int(transactionsNumberERC20), int(transactionsNumberETH), int(transactionsNumberSubsidy), 
                              int(transactionsNumberCustody), int(transactionsNumberCold), '', '', '',
                              int(transactionsNumberBEP20), int(transactionsNumberSubsidyBEP20), '', '', '', ''],
        '# of Transactors':[int(withdrawelersERC20), int(withdrawelersETH), '', '', '', '', '', '',
                            int(withdrawelersBSC), '', '', '', '', ''],
        'Avg. Transaction Cost, $': [round(transactionCostAvgERC20 * priceETH, 2), round(transactionCostAvgETH * priceETH, 2), 
                                    round(transactionCostAvgSubsidy * priceETH, 2), round(transactionCostAvgCustody * priceETH, 2), 
                                    round(transactionCostAvgCold * priceETH, 2), '', '', '',
                                    round(transactionCostAvgBEP20 * priceBNB, 4), round(transactionCostAvgSubsidyBEP20 * priceBNB, 4),
                                    '', '', '', ''],
        'Avg. Transaction Value, $': [round(withdrawalAvgERC20, 2), round(withdrawalAvgETH, 2), '', '', '', '', '', '',
                                      round(withdrawalAvgBSC, 2), '', '', '', '', ''],
        'Median Transaction Value, $': [round(withdrawalMedERC20, 2), round(withdrawalMedETH, 2), '', '', '', '', '', '',
                                        round(withdrawalMedBSC, 2), '', '', '', '', '']
    })

    # Saving
    finalTable.to_excel(f"{os.getcwd()}/gas_audit_{start}_{end}.xlsx", index = False)
