import psycopg2
import pandas as pd
pd.options.mode.chained_assignment = None
from datetime import date, datetime, timezone, timedelta
import requests
import os
from clickhouse_driver import Client
from credentials import dwh_user, dwh_pass, click_user, click_pass

# Instructions:
# 1. Set dwh credentials in a settings file
# 2. Set dates
# 3. Add or remove clients

# Notes:
# 1. Currently, there 4 quoted currencies in the script: USDT, BTC, ETH, TRX. 
# Amend script, if new quoted currencies appead.
# 2. Report is saved in the same folder where your script is.

# Setting the period
start = date(2021, 7, 19)
end = date(2021, 7, 25)

# Setting clients (exact order: currency, quote, client_id)
clients = pd.DataFrame([
    ['KAU', 'BTC', 'a7c582fe-a102-449e-a9e3-ac923844d103'],
    ['PHR', 'BTC', 'e962a8a9-1541-4bbe-b31b-87dbac17d7fb'],
    ['OAP', 'ETH', 'b55a150f-a564-41dd-bb12-765ba90187f0'],
    ['OAP', 'USDT', 'b55a150f-a564-41dd-bb12-765ba90187f0'],
    ['UNO', 'BTC', '45618247-7a95-47e0-9cce-d7e5b0deee4e'],
    ['UNO', 'USDT', '45618247-7a95-47e0-9cce-d7e5b0deee4e'],
    ['AVEX', 'ETH', '8fdd71e2-2103-4508-9a50-f6c56098c41c'],
    ['AVEX', 'USDT', '8fdd71e2-2103-4508-9a50-f6c56098c41c'],
    ['AVEX', 'BTC', '8fdd71e2-2103-4508-9a50-f6c56098c41c'],
    ['MAXI', 'ETH', '3a7d1efd-3afc-4372-b8bd-4d0d398837be'],
    ['DGB', 'USDT', 'af1fad15-d1c7-4008-bca9-40dbebc2d131'],
    ['DGB', 'BTC', 'af1fad15-d1c7-4008-bca9-40dbebc2d131'],
    ['PRDX', 'USDT', 'a55d4eef-1c12-4352-8adf-ac7f6e101ac1'],
    ['COM', 'USDT', '00629ea7-c45c-4565-adb6-5d8b23d1e7da'],
    ['COM', 'ETH', '00629ea7-c45c-4565-adb6-5d8b23d1e7da'],
    ['COM', 'BTC', '00629ea7-c45c-4565-adb6-5d8b23d1e7da'],
    ['NPO', 'BTC', '7b94b912-cb14-429d-b34d-5b8a08ce2333'],
    ['NIA', 'USDT', '480a55cc-fe1c-48b0-a2c3-4ab4628564d0'],
    ['RYIP', 'ETH', 'ce8ad3e3-99de-4cd6-9ece-ce9ae6294c60'],
    ['RYIP', 'USDT', 'ce8ad3e3-99de-4cd6-9ece-ce9ae6294c60'],
    ['BHF', 'USDT', 'c9198c8d-1895-4330-b8b3-7ddd9517f2b5'],
    ['ECTE', 'BTC', '38be40f9-5a55-46b2-9825-198598ad268a'],
    ['PQT', 'USDT', '7a60501f-27fe-43bb-95b7-d07b97718b4c'],
    ['EOC', 'USDT', '6c1d6519-1b93-48f4-9753-a23f2a9357d8'],
    ['PZS', 'USDT', 'c75f022c-8145-4da0-a509-fc5ef2a1793b'],
    ['WDR', 'USDT', 'a8b070c4-5380-4bf0-ad34-853bb2874758'],
    ['SHDC', 'USDT', 'be4a6181-cfd5-496b-b78d-629e9f4433d1'],
    ['SHDC', 'TRX', 'be4a6181-cfd5-496b-b78d-629e9f4433d1'],
    ['EMDC', 'USDT', 'ec568ff0-cda0-426f-b9ed-3026a3f244cb'],
    ['AZUM', 'USDT', 'a64ce810-289b-49b3-96ca-53de69e5f1c5'],
    ['PFI', 'USDT', '5a47e5b8-0191-4644-982f-9f9256571fd4'],
    ['FAN', 'USDT', '1d23f0c1-a63a-4cd7-8f30-b93736d0df0c'],
    ['EURU', 'USDT', '56da23a6-92de-465a-b381-89a5a5b69a33'],
    ['GBPU', 'USDT', '14669b7d-d3d0-4923-9aba-6def4f3289ee'],
    ['USDU', 'USDT', '01df89d0-9445-4efb-b965-3e1e297e95d6'],
    ['PBT', 'USDT', 'b6f34b8a-8e85-4422-b61c-ad2bc37957e9'],
    ['PBT', 'BTC', 'b6f34b8a-8e85-4422-b61c-ad2bc37957e9'],
    ['PBT', 'ETH', 'b6f34b8a-8e85-4422-b61c-ad2bc37957e9'],
    ['TRADE', 'USDT', '08fd9d50-7f48-4cae-b5c1-dd7095bd502b'],
    ['TRADE', 'ETH', '08fd9d50-7f48-4cae-b5c1-dd7095bd502b'],
    ['TRADE', 'BTC', '08fd9d50-7f48-4cae-b5c1-dd7095bd502b'],
    ['GCG', 'USDT', 'd4efaf0f-4cf8-4cdd-9bc2-b17a0e3f5dbd'],
    ['GCG', 'TRX', 'd4efaf0f-4cf8-4cdd-9bc2-b17a0e3f5dbd'],
    ['SFM', 'USDT', '6e72c428-658a-4ab1-956e-4997faa9790f'],
    ['ORE', 'USDT', '00b41a74-7079-4ead-b512-869146049742'],
    ['HLP', 'BTC', '542db3ea-d7e2-48ce-a770-6945e50628d2'],
    ['NECC', 'USDT', '706c17ff-6a77-4244-8cda-802be0d988b9c'],
    ['TRB', 'USDT', 'bbb492fb-baff-40b1-b719-352163505330'],
    ['YNI', 'USDT', 'f3c7a2d3-e66b-4d92-ae7f-852ce518adc9'],
    ['AIRX', 'USDT', '9d2373d4-b2e4-4980-8c93-0cf328002b26'],
    ['REAP', 'USDT', 'd371ab19-c436-4e90-9fb5-d80971f279c0'],
    ['BA', 'USDT', '1ace88f5-21d7-4ff0-8ff5-abe31060cb86'],
    ['NIT', 'ETH', '2baf7194-ddf4-43d9-bef6-f1ab955e3a32'],
    ['HVE2', 'USDT', '0988a56d-310a-4547-a27b-d8ec1bbda64e'],
    ['HVE2', 'ETH', '0988a56d-310a-4547-a27b-d8ec1bbda64e'],
    ['DFN', 'USDT', 'dfbcfdc4-5a83-4b27-ae11-268503dba149'],
    ['UNIS', 'TRX', '19c22c3a-a4d8-4e77-bb03-4fd29f91339d'],
    ['UNIS', 'BTC', '19c22c3a-a4d8-4e77-bb03-4fd29f91339d'],
    ['UNIS', 'USDT', '19c22c3a-a4d8-4e77-bb03-4fd29f91339d'],
    ['UNIS', 'ETH', '19c22c3a-a4d8-4e77-bb03-4fd29f91339d'],
    ['HCS', 'USDT', 'e756ac35-0a98-4b32-9fe3-c7892ec5662c'],
    ['SWAT', 'TRX', 'ed9cb2b3-e6fe-46c9-a438-54fc1b7898b8'],
    ['DRYCAKE', 'USDT', 'a0d922b8-7e87-4de4-b19a-62a9ab27bcff'],
    ['BN', 'ETH', 'be7ccd71-0623-4cc8-ae10-13a88c8e1f6c'],
    ['BN', 'ETH', 'be7ccd71-0623-4cc8-ae10-13a88c8e1f6c'],
    ['ABP', 'USDT', '2351320b-12e2-4563-b3d8-e99af4abd681'],
    ['RTF', 'USDT', 'c42296ee-a440-4d7d-95b5-1bc95fdfe26e'],
    ['NFD', 'USDT', '08776e00-daa0-4f52-b849-ca947f76b87f'],
    ['UCT', 'ETH', 'ed4dfd08-79cb-4682-b370-a9d90445d6d8'],
    ['DMX', 'USDT', '4e83b762-25a7-410d-b50b-8fe9b1f702f4'],
    ['GOL', 'USDT', '910971b2-f7e3-48f5-9d91-4e7221131ab0'],
    ['GOL', 'ETH', '910971b2-f7e3-48f5-9d91-4e7221131ab0'],
    ['GOL', 'BTC', '910971b2-f7e3-48f5-9d91-4e7221131ab0'],
    ['WHX', 'USDT', '1ab6af38-3a55-430a-aacf-cd853227bf1c'],
    ['AWT', 'USDT', 'c9139c49-7ef6-463c-9307-44a94dbca8e0'],
    ['CBT', 'USDT', 'a3842fd7-e422-4a4d-bfe0-47b5128a90c1'],
    ['SVX', 'USDT', '6b508fbe-685c-4ca2-a410-7e8680ec0057'],
    ['SVX', 'BTC', '6b508fbe-685c-4ca2-a410-7e8680ec0057'],
    ['OCTA', 'USDT', 'fdadba87-790b-4397-badc-439a05b8818f'],
    ['BCMC1', 'USDT', '1342a272-867f-4c78-bed1-f6c7cbbdceda'],
    ['WAR', 'TRX', '5a76a0b7-ec51-4de8-a43d-8fe3aa1a5cc5'],
    ['WAR', 'BTC', '5a76a0b7-ec51-4de8-a43d-8fe3aa1a5cc5'],
    ['TBT', 'USDT', '0a2f2de6-816f-4251-90f0-f3074fa8141c'],
    ['MAP', 'USDT', 'ac057d41-0294-4c72-a5a7-583ebd701867'],
    ['AVN', 'USDT', '1b760562-d97a-4913-b1ad-beee37b520e3'],
    ['SWAPS', 'USDT', '3e6e4f2d-100d-491c-87ac-506038c4ed9a'],
    ['CAC', 'USDT', '055ea382-1348-417b-b1a8-734fb29ea497'],
    ['GDOG', 'USDT', 'e293e5b8-03f1-423d-9e7c-b3cb5ee8e8b8'],
    ['WHXC', 'USDT', '1ab6af38-3a55-430a-aacf-cd853227bf1c'],
    ['GDM', 'USDT', '3851d00f-c581-47f8-8978-9970865c3d8e'],
    ['BBC', 'USDT', '5a19effc-89b5-4102-92b9-222e86731eef'],
    ['AOC', 'USDT', '3e2b0dec-60ad-42fb-8850-222db6ab8863'],
    ['AOC', 'BTC', '3e2b0dec-60ad-42fb-8850-222db6ab8863'],
    ['AOC', 'ETH', '3e2b0dec-60ad-42fb-8850-222db6ab8863'],
    ['ACTI', 'USDT', 'e656309f-4474-45a7-92f5-fe21fcb29c85'],
    ['HOHE', 'USDT', '1015b8b6-4e63-43a0-af25-9e3a69400db5'],
    ['GOLDUCK', 'USDT', '27b2e4c3-2765-4a82-b92b-c3d76e1dfdd8'],
    ['APL', 'BTC', '7bbe1e0a-ea8b-4cc8-a37a-c7da1d10f67c'],
    ['IJC', 'USDT', '57a1ac2d-7f0a-4412-b386-ebdc6058be3e'],
    ['IJC', 'ETH', '57a1ac2d-7f0a-4412-b386-ebdc6058be3e'],
    ['PBL', 'USDT', 'ca8942a4-771d-48a0-9059-92df75033205'],
    ['BRP', 'USDT', 'c126ea28-fa0a-4c9f-ac7e-ce3aa4c0a45e'],
    ['XRE', 'USDT', 'd664c2ba-e7df-4042-abc3-04971c7ce69e'],
    ['BRTR', 'USDT', '86fb6150-7038-440e-9cdd-c9630def9c91'],
    ['WACO', 'USDT', 'c2fe0564-d5d2-4e61-a73b-43a1f9dc2748'],
    ['WACO', 'BTC', 'c2fe0564-d5d2-4e61-a73b-43a1f9dc2748'],
    ['WACO', 'ETH', 'c2fe0564-d5d2-4e61-a73b-43a1f9dc2748'],
    ['LAS', 'USDT', '7399a265-5469-4f7e-8568-aca92660ffab'],
    ['KPC', 'USDT', 'acd35b5c-f737-4178-a6ec-07cd796c7eb4'],
    ['KISHU', 'TRX', 'fc022a03-a601-4b33-8cae-052904b0d087'],
    ['KWD', 'USDT', 'e8e07e38-ec05-415c-8d7d-9c71df39dffd'],
    ['CTC2', 'USDT', '0a371acb-6416-4070-aa46-6c8e927cbd57'],
    ['GEM', 'USDT', 'b73becfd-2e67-4cea-a93e-c9b89b0fc902'],
    ['ONYX', 'ETH', 'c599be7b-d759-41e2-aa91-62d39eac5702'],
    ['BBS', 'USDT', 'eef4a65a-b3f5-4ef5-b6f4-767b91b8a1a8'],
    ['PROMISE', 'USDT', '49128ae9-18ce-4299-8688-77c2efe129f8'],
    ['CREDIT', 'USDT', '07c7e693-22f6-49ff-8cc1-c3ac7002ea5b'],
    ['BOOMC', 'USDT', '3004297d-cb21-4a0c-a97d-d1738e7153f6'],
    ['BOOMC', 'ETH', '3004297d-cb21-4a0c-a97d-d1738e7153f6'],
    ['BOOMC', 'BTC', '3004297d-cb21-4a0c-a97d-d1738e7153f6'],
    ['BOOMC', 'TRX', '3004297d-cb21-4a0c-a97d-d1738e7153f6'],
    ['MOONSHOT', 'USDT', '24053ee9-ae07-414e-a7b4-2a0e7ecaa42c'],
    ['CT', 'BTC', '7da91f7b-bb26-4138-bce8-09bd7dab00d5'],
    ['CT', 'BTC', '7da91f7b-bb26-4138-bce8-09bd7dab00d5'],
    ['MYL', 'USDT', 'c371dbda-8d0c-4e70-b414-ebbb0996eea4'],
    ['DOGE2', 'USDT', '51a9e96e-cd42-4ef9-80cd-559e40f48470'],
    ['GMT', 'USDT', '15e1bbaf-ce8f-4115-ac26-1392198e94b8'],
    ['YSOY', 'USDT', 'fca0755f-44ee-4f3d-b65e-6b9c6511ccf2'],
    ['ILUS', 'USDT', 'c883aff3-6801-4dd9-bc71-e4fc635cb389'],
    ['ILUS', 'ETH', 'c883aff3-6801-4dd9-bc71-e4fc635cb389'],
    ['ILUS', 'BTC', 'c883aff3-6801-4dd9-bc71-e4fc635cb389'],
    ['FREN', 'USDT', 'a0360fe1-6418-4ee4-888a-56daa849c88e'],
    ['INVESTEL', 'USDT', '059cf5e5-25ee-4987-9128-349aafec3a66'],
    ['GTX', 'USDT', 'b132d147-d899-4ac5-af7f-f0156072040b'],
    ['SPHN', 'USDT', '3ce87638-f62e-475d-80c7-0329bf345d90'],
    ['CORGI', 'USDT', '30d7464e-209b-4f53-80ed-c06475785541'],
    ['HYBN', 'USDT', 'cf368372-886e-4c31-8dc8-477ed43ed3c9'],
    ['TOKAU', 'USDT', '86493617-e114-47f9-ad18-0ef3a9cb100e']
], columns = ['currency', 'quote', 'client_id'])


# Define functions
def run_dwh_sql(sql_query):
    conn = psycopg2.connect(host='135.181.61.116', user=dwh_user, password=dwh_pass, dbname='postgres',port='5432')
    cur = conn.cursor()
    cur.execute("SET statement_timeout = 0")
    data = pd.read_sql(sql_query, conn)
    conn.close()
    return data

client = Client("95.217.178.73", user=click_user, password=click_pass, database="default")

def get_cur_ids(tags):
    sql_query = f"""
    select tag, id
    from view_asset_manager_currency
    where tag in {tags};
    """ 
    return run_dwh_sql(sql_query)

def get_trades(start, end, currencies):
    sql_query = f"""
    select date(__create_date) as date, currency, quote, maker_trader, taker_trader, sum(cost), sum(maker_fee) as maker_fee, sum(taker_fee) as taker_fee
    from view_market_aggregator_trade
    where __update_datetime >= '{start} 00:00:00' and __update_datetime <= '{end} 23:59:59'
    and currency in {currencies}
    group by date, currency, quote, maker_trader, taker_trader
    order by date;
    """ 
    return run_dwh_sql(sql_query)

def spreads(pairs, start, end):
    mm_tr = f"""
        SELECT date, Symbol, avg(DiffPerc) AS averageSpread
        FROM minute_order_book
        WHERE Symbol IN {pairs}
        AND date >= '{start}'
        AND date <= '{end}'
        GROUP BY date, Symbol;
    """
    df = pd.DataFrame(client.execute(mm_tr))
    return df


# Running main part
if __name__ == '__main__':
    # Getting currency and quote ids
    unique_currencies = tuple(pd.unique(clients[['currency', 'quote']].values.ravel()))
    currency_ids = get_cur_ids(unique_currencies)
    currency_ids_dict = pd.Series(currency_ids.id.values, index=currency_ids.tag).to_dict()
    clients['currency_id'] = clients.currency.map(currency_ids_dict)
    clients['quote_id'] = clients.quote.map(currency_ids_dict)
    clients['pair'] = clients.currency + '/' + clients.quote
    clients['pairReq'] = clients.currency + clients.quote

    # Getting prices
    start_bin = int(str(int(datetime(start.year, start.month, start.day, 0, 0).replace(tzinfo=timezone.utc).timestamp()))+'000')
    end_bin = int(str(int(datetime(end.year, end.month, end.day, 0, 0).replace(tzinfo=timezone.utc).timestamp()))+'000')
    btc = requests.get(f"https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1d&startTime={start_bin}&endTime={end_bin}").json()
    eth = requests.get(f"https://api.binance.com/api/v3/klines?symbol=ETHUSDT&interval=1d&startTime={start_bin}&endTime={end_bin}").json()
    trx = requests.get(f"https://api.binance.com/api/v3/klines?symbol=TRXUSDT&interval=1d&startTime={start_bin}&endTime={end_bin}").json()
    
    # Getting trades
    print('Trades started')
    trades = get_trades(start, end, tuple(clients.currency_id.unique()))
    print('Trades received')
    # Assigning trade type
    trades.loc[trades[trades.taker_trader==trades.maker_trader].index, 'type'] = 'mm'
    trades.loc[trades[trades.taker_trader!=trades.maker_trader].index, 'type'] = 'organic'
            
    # Getting spreads
    print('Spreads started')
    totalSpreads = spreads(tuple(clients.pairReq.unique()), start, end).rename(columns = {0:'date', 1:'pair', 2:'spread'})
    totalSpreads.index = totalSpreads.date
    totalSpreads = totalSpreads.drop(columns=['date'])
    print('Spreads received')
    # Creating a saving object for Excel
    writer = pd.ExcelWriter(f'{os.getcwd()}/LS_report_{start.isoformat()}_to_{end.isoformat()}.xlsx')

    # Creating summary page
    firstPage = pd.DataFrame(data = 0, index = clients['pair'].unique(), 
        columns = ['Average Price', 'Volume Made, $', 'Volume Organic, $', 'Volume Total, $', 'Fees, $', 'Traders (by Trades)', 'Average Spread, %'])

    # Writing the data for each pair
    for i in range(len(clients)):
        
        pairOutput = pd.DataFrame(data = 0, index = pd.date_range(start, end), columns = ['Average Price'])

        # Volume
        tradesOutput = trades[(trades.currency==clients.currency_id.iloc[i])&(trades.quote==clients.quote_id.iloc[i])&
               ((trades.maker_trader==clients.client_id.iloc[i])|(trades.taker_trader==clients.client_id.iloc[i]))]

        mmVol = tradesOutput[tradesOutput.type=='mm'].groupby(['date'])['sum'].sum()
        retailVol = tradesOutput[tradesOutput.type=='organic'].groupby(['date'])['sum'].sum()

        pairOutput = pd.concat([pairOutput, mmVol], axis=1).rename(columns={'sum':'Volume Made, $'}).fillna(0)
        pairOutput = pd.concat([pairOutput, retailVol], axis=1).rename(columns={'sum':'Volume Organic, $'}).fillna(0)
        pairOutput['Volume Total, $'] = pairOutput['Volume Made, $'] + pairOutput['Volume Organic, $']

        # Fees
        feesMaker = trades[(trades.currency==clients.currency_id.iloc[i])&(trades.quote==clients.quote_id.iloc[i])&
               (trades.maker_trader==clients.client_id.iloc[i])]
        feesTaker = trades[(trades.currency==clients.currency_id.iloc[i])&(trades.quote==clients.quote_id.iloc[i])&
               (trades.taker_trader==clients.client_id.iloc[i])]

        makerFee = feesMaker.groupby(['date'])['maker_fee'].sum()
        takerFee = feesTaker.groupby(['date'])['taker_fee'].sum()
        fees = takerFee + makerFee

        pairOutput = pd.concat([pairOutput, fees], axis=1).rename(columns={0:'Fees, $'}).fillna(0)

        # Traders
        tradersMaker = tradesOutput[['date', 'maker_trader']].rename(columns={'maker_trader':'trader'})
        tradersTaker = tradesOutput[['date', 'taker_trader']].rename(columns={'taker_trader':'trader'})
        traders = pd.concat([tradersMaker, tradersTaker], ignore_index=True)
        traders = traders.groupby(['date'])['trader'].nunique()

        pairOutput = pd.concat([pairOutput, traders], axis=1).rename(columns={'trader':'Traders (by Trades)'}).fillna(0)

        # Spreads
        pairSpreads = totalSpreads[totalSpreads.pair==clients.pairReq.iloc[i]]['spread']
        pairOutput = pd.concat([pairOutput, pairSpreads], axis=1).rename(columns={'spread':'Average Spread, %'}).fillna(0)

        # Prices
        latokenPrices = requests.get(f'https://api.latoken.com/v2/tradingview/history?symbol={clients.currency.iloc[i]}%2F{clients.quote.iloc[i]}&resolution=1h&from={int(start_bin/1000)}&to={int(end_bin/1000)+86400}').json()

        for k in range(len(pairOutput)):
            priceList = list()
            for j in range(len(latokenPrices['t'])):
                if int(pairOutput.index[k].timestamp()+86400)>int(latokenPrices['t'][j]):
                    priceList.append(float(latokenPrices['c'][j]))
                else:
                    if len(priceList) == 0:
                        pairOutput.loc[pairOutput.index[k], 'Average Price'] = 0
                    else:
                        pairOutput.loc[pairOutput.index[k], 'Average Price'] = sum(priceList)/len(priceList)
                    break

        # Converting volume and fees in usdt
        pairOutput.columns.name = clients.pair.iloc[i]
        
        if pairOutput.columns.name.endswith('BTC'):
            for k in range(len(pairOutput)):
                for j in range(len(btc)):
                    if int(pairOutput.index[k].timestamp()*1000) == int(btc[j][0]):
                        pairOutput.loc[pairOutput.index[k], 'Fees, $'] = pairOutput.loc[pairOutput.index[k], 'Fees, $'] * float(btc[j][4])
                        pairOutput.loc[pairOutput.index[k], 'Volume Total, $'] = pairOutput.loc[pairOutput.index[k], 'Volume Total, $'] * float(btc[j][4])
                        pairOutput.loc[pairOutput.index[k], 'Volume Made, $'] = pairOutput.loc[pairOutput.index[k], 'Volume Made, $'] * float(btc[j][4])
                        pairOutput.loc[pairOutput.index[k], 'Volume Organic, $'] = pairOutput.loc[pairOutput.index[k], 'Volume Organic, $'] * float(btc[j][4])
                    else:
                        continue

        if pairOutput.columns.name.endswith('ETH'):
            for k in range(len(pairOutput)):
                for j in range(len(eth)):
                    if int(pairOutput.index[k].timestamp()*1000) == int(eth[j][0]):
                        pairOutput.loc[pairOutput.index[k], 'Fees, $'] = pairOutput.loc[pairOutput.index[k], 'Fees, $'] * float(eth[j][4])
                        pairOutput.loc[pairOutput.index[k], 'Volume Total, $'] = pairOutput.loc[pairOutput.index[k], 'Volume Total, $'] * float(eth[j][4])
                        pairOutput.loc[pairOutput.index[k], 'Volume Made, $'] = pairOutput.loc[pairOutput.index[k], 'Volume Made, $'] * float(eth[j][4])
                        pairOutput.loc[pairOutput.index[k], 'Volume Organic, $'] = pairOutput.loc[pairOutput.index[k], 'Volume Organic, $'] * float(eth[j][4])
                    else:
                        continue   

        if pairOutput.columns.name.endswith('TRX'):
            for k in range(len(pairOutput)):
                for j in range(len(trx)):
                    if int(pairOutput.index[k].timestamp()*1000) == int(trx[j][0]):
                        pairOutput.loc[pairOutput.index[k], 'Fees, $'] = pairOutput.loc[pairOutput.index[k], 'Fees, $'] * float(trx[j][4])
                        pairOutput.loc[pairOutput.index[k], 'Volume Total, $'] = pairOutput.loc[pairOutput.index[k], 'Volume Total, $'] * float(trx[j][4])
                        pairOutput.loc[pairOutput.index[k], 'Volume Made, $'] = pairOutput.loc[pairOutput.index[k], 'Volume Made, $'] * float(trx[j][4])
                        pairOutput.loc[pairOutput.index[k], 'Volume Organic, $'] = pairOutput.loc[pairOutput.index[k], 'Volume Organic, $'] * float(trx[j][4])
                    else:
                        continue
        
        print(f'Done for {pairOutput.columns.name}')
        pairOutput.to_excel(writer, sheet_name=pairOutput.columns.name.split('/')[0] + pairOutput.columns.name.split('/')[1], index = True)

        firstPage.loc[pairOutput.columns.name, 'Average Price'] = pairOutput['Average Price'].mean()
        firstPage.loc[pairOutput.columns.name, 'Volume Made, $'] = pairOutput['Volume Made, $'].sum()
        firstPage.loc[pairOutput.columns.name, 'Volume Organic, $'] = pairOutput['Volume Organic, $'].sum()
        firstPage.loc[pairOutput.columns.name, 'Volume Total, $'] = pairOutput['Volume Total, $'].sum()
        firstPage.loc[pairOutput.columns.name, 'Fees, $'] = pairOutput['Fees, $'].sum()
        firstPage.loc[pairOutput.columns.name, 'Traders (by Trades)'] = len(pd.unique(tradesOutput[['maker_trader', 'taker_trader']].values.ravel()))
        firstPage.loc[pairOutput.columns.name, 'Average Spread, %'] = pairOutput['Average Spread, %'].mean()

    firstPage.to_excel(writer, sheet_name='weekly summary', index = True)
    writer.save()



