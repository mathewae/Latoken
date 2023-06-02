from time import sleep
import requests
import pandas as pd
pd.options.mode.chained_assignment = None
import os
import psycopg2
from datetime import datetime, timezone, timedelta
from credentials import dwh_user, dwh_pass


def amountsNFees():
    sql_query = f"""
    select vamc.tag, vtmppl.name as chain, vtmcbl.type as binding, vtmcbl.min_amount as minamount, 
            vtmcbl.fee as fee, percent_fee
    from view_transaction_manager_currency_binding_latest vtmcbl
    join view_asset_manager_currency vamc
        on vamc.id = vtmcbl.currency
    join view_transaction_manager_currency_provider_latest vtmcpl
        on vtmcbl.currency_provider_id = vtmcpl.id
    join view_transaction_manager_payment_provider_latest vtmppl
        on vtmcpl.provider_id = vtmppl.id
    where vtmcbl.status = 'ACTIVE' and vamc.status = 'ACTIVE';
    """
    return run_dwh_sql(sql_query)

def run_dwh_sql(sql_query):
    conn = psycopg2.connect(host='135.181.61.116', user=dwh_user, password=dwh_pass, dbname='postgres',port='5432')
    cur = conn.cursor()
    cur.execute("SET statement_timeout = 0")
    data = pd.read_sql(sql_query, conn)
    conn.close()
    return data

# 'ERC20', 'BSC_TOKEN', 'TRC20' or 'TRC10'

# Variables
minamountETH = 10
minamountBSC = 5
minamountTRON = 2.5
minamountOther = 10
fixedFeesDep = 0
fixedFeesWithETH = 10
fixedFeesWithBSC = 5
fixedFeesWithTRON = 2.5
fixedFeesWithOther = 10
percFeeDep = 1
percFeeWith = 1
gasEstimateForOther = 1

gweiConverter = 0.000000001
gweiRequired = 65000
gweiRequiredBSC = 20
RequiredSun = 7143

top30 = ['BTC', 'ETH', 'TRX', 'USDT', 'DOGE', 'ADA', 'XRP', 'USDC', 'DOT', 'UNI', 'BCH', 'SOL', 'LTC',
        'LINK', 'MATIC', 'XLM', 'VET', 'XMR', 'EOS', 'SHIB', 'AAVE'] # From CMC
deflationaryTokens = [] # Add here

# Deposits
# No fixed fee
# 1% fee for all tokens except for Top 30
# Minamount is $7 (+-$2 for price fluctuations) 
### Mainly also useless as we spend the same gas on $1 and $1000 collection
### Mainly to cover gas, if gas exceeds, then minamount X times by dk

# Withdrawals
# Fixed fee $5 (x2 for deflationary tokens)
# 1% fee for all tokens except for USDT, equities, ETH, BTC (X% for deflationary tokens) 
###+ we need a warning at withdrawal that you will receive less tokens due to deflation
# Minamount $7 (+-$2 for price fluctuations) (mainly useless)
# bitcoin (txo - потратить)

# Process for those who deposited less than min amount or something that is not listed.
###

if __name__ == '__main__':
    
    # Get data
    data = amountsNFees()
    
    # Getting LATOKEN Prices
    start_lat = int((datetime.now() - timedelta(days=1, hours=3)
                    ).replace(tzinfo=timezone.utc).timestamp())
    end_lat = int((datetime.now() - timedelta(hours=3)
                  ).replace(tzinfo= timezone.utc).timestamp())

    latPrices = dict()
    missing = list()

    btcLat = float(requests.get(f'https://api.latoken.com/v2/ticker/BTC/USDT').json()['lastPrice'])
    ethLat = float(requests.get(f'https://api.latoken.com/v2/ticker/ETH/USDT').json()['lastPrice'])
    trxLat = float(requests.get(f'https://api.latoken.com/v2/ticker/TRX/USDT').json()['lastPrice'])
    bnbBin = float(requests.get(f'https://api.binance.com/api/v3/avgPrice?symbol=BNBUSDT').json()['price'])

    for i in range(len(data.tag.unique())):
        if data.tag.unique()[i] == 'USDT':
            latPrices[data.tag.unique()[i]] = 1
        else:
            try:
                lat = float(requests.get(f'https://api.latoken.com/v2/ticker/{data.tag.unique()[i]}/USDT').json()['lastPrice'])
                latPrices[data.tag.unique()[i]] = lat
            except:
                try:
                    lat = float(requests.get(f'https://api.latoken.com/v2/ticker/{data.tag.unique()[i]}/BTC').json()['lastPrice'])
                    latPrices[data.tag.unique()[i]] = lat * btcLat
                except:
                    try:
                        lat = float(requests.get(f'https://api.latoken.com/v2/ticker/{data.tag.unique()[i]}/ETH').json()['lastPrice'])
                        latPrices[data.tag.unique()[i]] = lat * ethLat
                    except: 
                        try:
                            lat = float(requests.get(f'https://api.latoken.com/v2/ticker/{data.tag.unique()[i]}/TRX').json()['lastPrice'])
                            latPrices[data.tag.unique()[i]] = lat * trxLat
                        except:
                            latPrices[data.tag.unique()[i]] = 0
                            missing.append(data.tag.unique()[i])
                            
    data['prices'] = data.tag.map(latPrices).fillna(0)
    
    # Get gas prices for ETH, BSC, Tron
    transactionCost = 0
    while not isinstance(transactionCost, float):
        sleep(0.5)
        gasPrice = float(requests.get("https://api.etherscan.io/api?module=gastracker&action=gasoracle").json()['result']['SafeGasPrice'])
        transactionCost = gasPrice * gweiRequired * ethLat * gweiConverter
        
    transactionCostBSC = 0
    while not isinstance(transactionCostBSC, float):
        sleep(0.5)
        # gasPriceBSC = requests.get("https://bscgas.info/gas").json()['standard']
        transactionCostBSC = 0.000223 * bnbBin # gasPriceBSC * gweiRequiredBSC * bnbBin * gweiConverter
        
    transactionCostTron = 0
    while not isinstance(transactionCostTron, float):
        tronTrans = requests.get(f'https://apilist.tronscan.org/api/transaction?sort=-timestamp&count=true&limit=20&start_timestamp={int(datetime.now().timestamp())}&address=TT2YwaJ8DXsrpycgBGDWEei1FUQm6YT85T').json()
        
        energy = 0
        for i in range(len(tronTrans['data'])):
            try:
                if tronTrans['data'][i]['cost']['energy_usage_total'] != 0:
                    energy = tronTrans['data'][i]['cost']['energy_usage_total']
                    break
                else:
                    continue
            except:
                continue
        
        transactionCostTron = (energy/RequiredSun) * trxLat

    # Split into chains
    etherChainDep = data[(data.chain.isin(['ERC20', 'ETH']))&(data.binding=='INPUT')]
    etherChainWith = data[(data.chain.isin(['ERC20', 'ETH']))&(data.binding=='OUTPUT')]
    bscChainDep = data[(data.chain == 'BSC_TOKEN')&(data.binding=='INPUT')]
    bscChainWith = data[(data.chain == 'BSC_TOKEN')&(data.binding=='OUTPUT')]
    tronChainDep = data[(data.chain.isin(['TRC20', 'TRC10', 'TRON']))&(data.binding=='INPUT')]
    tronChainWith = data[(data.chain.isin(['TRC20', 'TRC10', 'TRON']))&(data.binding=='OUTPUT')]
    otherChainsDep = data[~(data.chain.isin(['TRC20','TRC10','BSC_TOKEN','ERC20', 'ETH', 'TRON']))&(data.binding=='INPUT')]
    otherChainsWith = data[~(data.chain.isin(['TRC20','TRC10','BSC_TOKEN','ERC20', 'ETH', 'TRON']))&(data.binding=='OUTPUT')]


    ###DEPOSITS###
    # Compare minamount with requrement
    etherChainDep['minAmountReq'] = minamountETH
    etherChainDep['mindepositUSD'] = etherChainDep.minamount * etherChainDep.prices
    etherChainDep['requirementExceeds'] = etherChainDep.mindepositUSD - etherChainDep.minAmountReq
    # Compare minamount with gas price
    etherChainDep['gas'] = transactionCost
    etherChainDep['gasExceeds'] = etherChainDep.mindepositUSD - etherChainDep.gas

    ethDepSum = [len(etherChainDep),
    len(etherChainDep[etherChainDep.gasExceeds<0]),
    len(etherChainDep[etherChainDep.requirementExceeds<0]),
    len(etherChainDep[etherChainDep.percent_fee!=percFeeDep]),
    len(etherChainDep[etherChainDep.fee!=fixedFeesDep]),
    len(etherChainDep[etherChainDep.prices==0])]

    # Compare minamount with requrement
    bscChainDep['minAmountReq'] = minamountBSC
    bscChainDep['mindepositUSD'] = bscChainDep.minamount * bscChainDep.prices
    bscChainDep['requirementExceeds'] = bscChainDep.mindepositUSD - bscChainDep.minAmountReq
    # Compare minamount with gas price
    bscChainDep['gas'] = transactionCostBSC
    bscChainDep['gasExceeds'] = bscChainDep.mindepositUSD - bscChainDep.gas

    bscDepSum = [len(bscChainDep),
    len(bscChainDep[bscChainDep.gasExceeds<0]),
    len(bscChainDep[bscChainDep.requirementExceeds<0]),
    len(bscChainDep[bscChainDep.percent_fee!=percFeeDep]),
    len(bscChainDep[bscChainDep.fee!=fixedFeesDep]),
    len(bscChainDep[bscChainDep.prices==0])]


    # Compare minamount with requrement
    tronChainDep['minAmountReq'] = minamountTRON
    tronChainDep['mindepositUSD'] = tronChainDep.minamount * tronChainDep.prices
    tronChainDep['requirementExceeds'] = tronChainDep.mindepositUSD - tronChainDep.minAmountReq
    # Compare minamount with gas price
    tronChainDep['gas'] = transactionCostTron
    tronChainDep['gasExceeds'] = tronChainDep.mindepositUSD - tronChainDep.gas

    tronDepSum = [len(tronChainDep),
    len(tronChainDep[tronChainDep.gasExceeds<0]),
    len(tronChainDep[tronChainDep.requirementExceeds<0]),
    len(tronChainDep[tronChainDep.percent_fee!=percFeeDep]),
    len(tronChainDep[tronChainDep.fee!=fixedFeesDep]),
    len(tronChainDep[tronChainDep.prices==0])]


    # Compare minamount with requrement
    otherChainsDep['minAmountReq'] = minamountOther
    otherChainsDep['mindepositUSD'] = otherChainsDep.minamount * otherChainsDep.prices
    otherChainsDep['requirementExceeds'] = otherChainsDep.mindepositUSD - otherChainsDep.minAmountReq
    # Compare minamount with gas price
    otherChainsDep['gas'] = gasEstimateForOther
    otherChainsDep['gasExceeds'] = otherChainsDep.mindepositUSD - otherChainsDep.gas

    otherDepSum = [len(otherChainsDep),
    len(otherChainsDep[otherChainsDep.gasExceeds<0]),
    len(otherChainsDep[otherChainsDep.requirementExceeds<0]),
    len(otherChainsDep[otherChainsDep.percent_fee!=percFeeDep]),
    len(otherChainsDep[otherChainsDep.fee!=fixedFeesDep]),
    len(otherChainsDep[otherChainsDep.prices==0])]


    # Aggregate active deposits table
    tableDep = pd.DataFrame(index = ['Ethereum', 'BSC', 'Tron', 'Other'], columns = ['Total', 'Gas_not_covered', 'Requirement_not_covered', '%_fee_incorrect', 'fixed_fee_incorrect', '0_price'])
    tableDep.columns.name = 'Active Deposit Blockchains'
    tableDep.loc['Ethereum', :] = ethDepSum
    tableDep.loc['BSC', :] = bscDepSum
    tableDep.loc['Tron', :] = tronDepSum
    tableDep.loc['Other', :] = otherDepSum


    ###WITHDRAWALS###
    # Compare minamount with requrement
    etherChainWith['minAmountReq'] = minamountETH
    etherChainWith['mindepositUSD'] = etherChainWith.minamount * etherChainWith.prices
    etherChainWith['requirementExceeds'] = etherChainWith.mindepositUSD - etherChainWith.minAmountReq
    # Compare minamount with gas price
    etherChainWith['gas'] = transactionCost
    etherChainWith['gasExceeds'] = etherChainWith.mindepositUSD - etherChainWith.gas

    ethWithSum = [len(etherChainWith),
    len(etherChainWith[etherChainWith.gasExceeds<0]),
    len(etherChainWith[etherChainWith.requirementExceeds<0]),
    len(etherChainWith[etherChainWith.percent_fee!=percFeeWith]),
    len(etherChainWith[etherChainWith.fee!=fixedFeesWithETH]),
    len(etherChainWith[etherChainWith.prices==0])]


    # Compare minamount with requrement
    bscChainWith['minAmountReq'] = minamountBSC
    bscChainWith['mindepositUSD'] = bscChainWith.minamount * bscChainWith.prices
    bscChainWith['requirementExceeds'] = bscChainWith.mindepositUSD - bscChainWith.minAmountReq
    # Compare minamount with gas price
    bscChainWith['gas'] = transactionCostBSC
    bscChainWith['gasExceeds'] = bscChainWith.mindepositUSD - bscChainWith.gas

    bscWithSum = [len(bscChainWith),
    len(bscChainWith[bscChainWith.gasExceeds<0]),
    len(bscChainWith[bscChainWith.requirementExceeds<0]),
    len(bscChainWith[bscChainWith.percent_fee!=percFeeWith]),
    len(bscChainWith[bscChainWith.fee!=fixedFeesWithBSC]),
    len(bscChainWith[bscChainWith.prices==0])]


    # Compare minamount with requrement
    tronChainWith['minAmountReq'] = minamountTRON
    tronChainWith['mindepositUSD'] = tronChainWith.minamount * tronChainWith.prices
    tronChainWith['requirementExceeds'] = tronChainWith.mindepositUSD - tronChainWith.minAmountReq
    # Compare minamount with gas price
    tronChainWith['gas'] = transactionCostTron
    tronChainWith['gasExceeds'] = tronChainWith.mindepositUSD - tronChainWith.gas

    tronWithSum = [len(tronChainWith),
    len(tronChainWith[tronChainWith.gasExceeds<0]),
    len(tronChainWith[tronChainWith.requirementExceeds<0]),
    len(tronChainWith[tronChainWith.percent_fee!=percFeeWith]),
    len(tronChainWith[tronChainWith.fee!=fixedFeesWithTRON]),
    len(tronChainWith[tronChainWith.prices==0])]


    # Compare minamount with requrement
    otherChainsWith['minAmountReq'] = minamountOther
    otherChainsWith['mindepositUSD'] = otherChainsWith.minamount * otherChainsWith.prices
    otherChainsWith['requirementExceeds'] = otherChainsWith.mindepositUSD - otherChainsWith.minAmountReq
    # Compare minamount with gas price
    otherChainsWith['gas'] = gasEstimateForOther
    otherChainsWith['gasExceeds'] = otherChainsWith.mindepositUSD - otherChainsWith.gas

    otherWithSum = [len(otherChainsWith),
    len(otherChainsWith[otherChainsWith.gasExceeds<0]),
    len(otherChainsWith[otherChainsWith.requirementExceeds<0]),
    len(otherChainsWith[otherChainsWith.percent_fee!=percFeeWith]),
    len(otherChainsWith[otherChainsWith.fee!=fixedFeesWithOther]),
    len(otherChainsWith[otherChainsWith.prices==0])]


    # Aggregate active withdrawals table
    tableWith = pd.DataFrame(index = ['Ethereum', 'BSC', 'Tron', 'Other'], columns = ['Total', 'Gas_not_covered', 'Requirement_not_covered', '%_fee_incorrect', 'fixed_fee_incorrect', '0_price'])
    tableWith.columns.name = 'Active Withdrawal Blockchains'
    tableWith.loc['Ethereum', :] = ethWithSum
    tableWith.loc['BSC', :] = bscWithSum
    tableWith.loc['Tron', :] = tronWithSum
    tableWith.loc['Other', :] = otherWithSum


    # Saving
    writer = pd.ExcelWriter(f'{os.getcwd()}/bindingsReport_{datetime.now()}.xlsx')
    tableDep.to_excel(writer, sheet_name='DepositsSummary', index = True)
    tableWith.to_excel(writer, sheet_name='WithdrawalsSummary', index = True)
    etherChainDep.to_excel(writer, sheet_name='ethereumDep', index = False)
    bscChainDep.to_excel(writer, sheet_name='bscDep', index = False)
    tronChainDep.to_excel(writer, sheet_name='tronDep', index = False)
    otherChainsDep.to_excel(writer, sheet_name='otherDep', index = False)
    etherChainWith.to_excel(writer, sheet_name='ethereumWith', index = False)
    bscChainWith.to_excel(writer, sheet_name='bscWith', index = False)
    tronChainWith.to_excel(writer, sheet_name='tronWith', index = False)
    otherChainsWith.to_excel(writer, sheet_name='otherWith', index = False)
    writer.save()

