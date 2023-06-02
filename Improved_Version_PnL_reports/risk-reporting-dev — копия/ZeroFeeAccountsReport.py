import psycopg2
import pandas as pd
pd.options.mode.chained_assignment = None
import datetime as dt
from clickhouse_driver import Client
import requests
import os
from credentials import dwh_user, dwh_pass, click_user, click_pass


# Inputs
clients = pd.DataFrame([
    ['BFIC', 'USDT', '59d8d249-cf70-4fb2-92c9-f2d46cb5bfc1'],
    ['BFIC', 'USDT', '9cae9596-26a8-41ae-8a56-04931136aae7'],
    ['AOC', 'USDT', '3e2b0dec-60ad-42fb-8850-222db6ab8863'],
    ['AOC', 'BTC', '3e2b0dec-60ad-42fb-8850-222db6ab8863'],
    ['AOC', 'ETH', '3e2b0dec-60ad-42fb-8850-222db6ab8863'],
    ['FESS', 'USDT', '762efaef-3f3e-4c18-a78e-b9b45f60fe4d'],
    ['FESS', 'USDT', '2555c369-0379-4c14-bcfb-333230fa1dd8'],
    ['ALPACA', 'USDT', '7a081464-ae89-41da-905b-008aaff385fa'],
    ['MCAN', 'USDT', '77f5758e-757f-46a3-8e6f-a7e949a00b44'],
    ['MCAN', 'BTC', '77f5758e-757f-46a3-8e6f-a7e949a00b44'],
    ['MCAN', 'ETH', '77f5758e-757f-46a3-8e6f-a7e949a00b44'],
    ['MTRAX', 'USDT', 'd6cd0ab7-2016-4128-97e1-fe1230775719'],
    ['CT', 'USDT', '7da91f7b-bb26-4138-bce8-09bd7dab00d5'], # Example (currency, quote, client id)
], columns = ['currency', 'quote', 'client_id'])

start = dt.date(2021, 7, 19)
end = dt.date(2021, 7, 25)

volume_needed = 20000 # This is the volume a pair should break over daily
spread_needed = 2 # This is the spread a pair should break over daily


# Defining functions
def get_trades(start, end, currencies):
    sql_query = f"""
    select date(__update_datetime) as date, currency, quote, maker_trader, taker_trader, sum(cost) as volume
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

def get_cur_ids(tags):
    sql_query = f"""
    select tag, id
    from view_asset_manager_currency
    where tag in {tags};
    """ 
    return run_dwh_sql(sql_query)

def zeroFeesCurrent():
    sql_query = f"""
    select __create_date, user_id, base_currency_id, quote_currency_id, start_time, finish_time, status
    from view_fee_manager_fee_schedule
    where fee_group_id = '734d834f-773e-4e5a-9bf0-054e8f3f88c0' and status = 'ACTIVE' and finish_time >= now()
    order by finish_time desc;
    """
    return run_dwh_sql(sql_query)

def zeroFeesExpired():
    sql_query = f"""
    select __create_date, user_id, base_currency_id, quote_currency_id, start_time, finish_time, status
    from view_fee_manager_fee_schedule
    where fee_group_id = '734d834f-773e-4e5a-9bf0-054e8f3f88c0' and status = 'ACTIVE' and finish_time <= now()
    order by finish_time desc;
    """
    return run_dwh_sql(sql_query)

def get_cur_tags(x):
    sql_query = f"""
    select id, tag
    from view_asset_manager_currency
    where id in {x};
    """
    return run_dwh_sql(sql_query)


if __name__ == '__main__':
    # Establishing connections
    def run_dwh_sql(sql_query):
        conn = psycopg2.connect(host='135.181.61.116', user=dwh_user, password=dwh_pass, dbname='postgres',port='5432')
        data = pd.read_sql(sql_query, conn)
        conn.close()
        return data

    client = Client("95.217.178.73", user=click_user, password=click_pass, database="default")

    # Check the length of the clients table (if it is less than 2 unique currencies, then there will be a problem quering to db)
    if len(clients.currency.unique())==0:
        print("You didn't specify any clients")
        exit()
    elif len(clients.currency.unique())==1:
        addition = pd.DataFrame([
            ['USDT', 'USDT', 'e962a8a9-1541-4bbe-b31b-87dbac17d7fb']
        ], columns = ['currency', 'quote', 'client_id'])
        clients = clients.append(addition, ignore_index=True)
        del addition
    else:
        pass

    # Getting currency and quote ids
    unique_currencies = tuple(pd.unique(clients[['currency', 'quote']].values.ravel()))
    currency_ids = get_cur_ids(unique_currencies)
    currency_ids_dict = pd.Series(currency_ids.id.values, index=currency_ids.tag).to_dict()
    clients['currency_id'] = clients.currency.map(currency_ids_dict)
    clients['quote_id'] = clients.quote.map(currency_ids_dict)
    clients['pair'] = clients.currency + clients.quote
    clients['done'] = 0 # Creating a check column

    # Getting spreads
    unique_pairs = tuple(pd.unique((clients['currency'] + clients['quote']).values.ravel()))
    spreadsWeek = spreads(unique_pairs, start, end)
    spreadsWeek.index = spreadsWeek[0]

    # Getting volumes
    volumes = get_trades(start, end, tuple(clients.currency_id.unique()))

    # Getting prices
    start_bin = int(str(int(dt.datetime(start.year, start.month, start.day, 0, 0).replace(tzinfo=dt.timezone.utc).timestamp()))+'000')
    end_bin = int(str(int(dt.datetime(end.year, end.month, end.day, 0, 0).replace(tzinfo=dt.timezone.utc).timestamp()))+'000')
    btc = requests.get(f"https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1d&startTime={start_bin}&endTime={end_bin}").json()
    eth = requests.get(f"https://api.binance.com/api/v3/klines?symbol=ETHUSDT&interval=1d&startTime={start_bin}&endTime={end_bin}").json()
    trx = requests.get(f"https://api.binance.com/api/v3/klines?symbol=TRXUSDT&interval=1d&startTime={start_bin}&endTime={end_bin}").json()

    # Wrangling data to get the final result by pair by date
    final = pd.DataFrame(columns=['volume', 'pair', 'spread']) # Final dataframe
    for i in range(len(clients)):
        # First, if the 'done' column is checked, then we should move on to the next row
        if len(clients[(clients.pair==clients.pair.iloc[i])&(clients.done==0)].client_id) == 0:
            continue
        
        # Secondly, if the 'done' column is unchecked we start processing data.
        # We, firstly, count whether there is one or more accounts used by the client to do market-making. This is
        # important because some clients market-make from 2 or more accounts in one pair. So we have to add up numbers
        # from all of their accounts to finally decide whether they fulfilled our requirements pairwise or not.
        elif len(clients[(clients.pair==clients.pair.iloc[i])&(clients.done==0)].client_id) == 1:
            # Inside we then determine the quote currency to correctly convert volumes into usd.
            if clients.quote_id.iloc[i] == '0c3a106d-bde3-4c13-a26e-3fd2394529e5':
                finals = pd.DataFrame(volumes[(volumes.currency==clients.currency_id.iloc[i])&
                   (volumes.quote==clients.quote_id.iloc[i])&
                   ((volumes.maker_trader==clients.client_id.iloc[i])|
                   (volumes.taker_trader==clients.client_id.iloc[i]))].groupby(['date'])['volume'].sum())
                finals = finals.join(spreadsWeek[spreadsWeek[1]==clients.pair.iloc[i]]).rename(
                    {1: 'pair', 2: 'spread'}, axis=1).fillna(0)
                finals = finals.drop(columns = [0])
                final = final.append(finals)
                del finals
            
            elif clients.quote_id.iloc[i] == '92151d82-df98-4d88-9a4d-284fa9eca49f':
                finals = pd.DataFrame(volumes[(volumes.currency==clients.currency_id.iloc[i])&
                   (volumes.quote==clients.quote_id.iloc[i])&
                   ((volumes.maker_trader==clients.client_id.iloc[i])|
                   (volumes.taker_trader==clients.client_id.iloc[i]))].groupby(['date'])['volume'].sum())
                finals = finals.join(spreadsWeek[spreadsWeek[1]==clients.pair.iloc[i]]).rename(
                    {1: 'pair', 2: 'spread'}, axis=1).fillna(0)
                finals = finals.drop(columns = [0])
                for k in range(len(finals)):
                    finals.volume.iloc[k] = finals.volume.iloc[k] * float(btc[(finals.index[k] - start).days][4])
                final = final.append(finals)
                del finals
            
            elif clients.quote_id.iloc[i] == '620f2019-33c0-423b-8a9d-cde4d7f8ef7f':
                finals = pd.DataFrame(volumes[(volumes.currency==clients.currency_id.iloc[i])&
                   (volumes.quote==clients.quote_id.iloc[i])&
                   ((volumes.maker_trader==clients.client_id.iloc[i])|
                   (volumes.taker_trader==clients.client_id.iloc[i]))].groupby(['date'])['volume'].sum())
                finals = finals.join(spreadsWeek[spreadsWeek[1]==clients.pair.iloc[i]]).rename(
                    {1: 'pair', 2: 'spread'}, axis=1).fillna(0)
                finals = finals.drop(columns = [0])
                for k in range(len(finals)):
                    finals.volume.iloc[k] = finals.volume.iloc[k] * float(eth[(finals.index[k] - start).days][4])
                final = final.append(finals)
                del finals
            
            elif clients.quote_id.iloc[i] == '34629b4b-753c-4537-865f-4b62ff1a31d6':
                finals = pd.DataFrame(volumes[(volumes.currency==clients.currency_id.iloc[i])&
                   (volumes.quote==clients.quote_id.iloc[i])&
                   ((volumes.maker_trader==clients.client_id.iloc[i])|
                   (volumes.taker_trader==clients.client_id.iloc[i]))].groupby(['date'])['volume'].sum())
                finals = finals.join(spreadsWeek[spreadsWeek[1]==clients.pair.iloc[i]]).rename(
                    {1: 'pair', 2: 'spread'}, axis=1).fillna(0)
                finals = finals.drop(columns = [0])
                for k in range(len(finals)):
                    finals.volume.iloc[k] = finals.volume.iloc[k] * float(trx[(finals.index[k] - start).days][4])
                final = final.append(finals)
                del finals
                
            clients.loc[(clients.pair==clients.pair.iloc[i]), 'done'] = 1 # At the end we add a checkmark to the 'done' column
        
        # Finally, we process pairs with multiple accounts for market-making.
        elif len(clients[clients.pair==clients.pair.iloc[i]].client_id) > 1:
            finals_inter = pd.DataFrame(None) # This is intermediary table needed to sum-up multiple accounts data
            for j in range(len(clients[clients.pair==clients.pair.iloc[i]].client_id)):
                if clients.quote_id.iloc[i] == '0c3a106d-bde3-4c13-a26e-3fd2394529e5':
                    finals = pd.DataFrame(volumes[(volumes.currency==clients.currency_id.iloc[i])&
                       (volumes.quote==clients.quote_id.iloc[i])&
                       ((volumes.maker_trader==clients[clients.pair==clients.pair.iloc[i]].client_id.iloc[j])|
                       (volumes.taker_trader==clients[clients.pair==clients.pair.iloc[i]].client_id.iloc[j]))].groupby(
                        ['date'])['volume'].sum())
                    finals_inter = pd.concat([finals_inter, finals], axis=1).fillna(0).astype(int)
                    del finals
            
                elif clients.quote_id.iloc[i] == '92151d82-df98-4d88-9a4d-284fa9eca49f':
                    finals = pd.DataFrame(volumes[(volumes.currency==clients.currency_id.iloc[i])&
                       (volumes.quote==clients.quote_id.iloc[i])&
                       ((volumes.maker_trader==clients[clients.pair==clients.pair.iloc[i]].client_id.iloc[j])|
                       (volumes.taker_trader==clients[clients.pair==clients.pair.iloc[i]].client_id.iloc[j]))].groupby(
                        ['date'])['volume'].sum())
                    for k in range(len(finals)):
                        finals.volume.iloc[k] = finals.volume.iloc[k] * float(btc[(finals.index[k] - start).days][4])
                    finals_inter = pd.concat([finals_inter, finals], axis=1).fillna(0).astype(int)
                    del finals
                
                elif clients.quote_id.iloc[i] == '620f2019-33c0-423b-8a9d-cde4d7f8ef7f':
                    finals = pd.DataFrame(volumes[(volumes.currency==clients.currency_id.iloc[i])&
                       (volumes.quote==clients.quote_id.iloc[i])&
                       ((volumes.maker_trader==clients[clients.pair==clients.pair.iloc[i]].client_id.iloc[j])|
                       (volumes.taker_trader==clients[clients.pair==clients.pair.iloc[i]].client_id.iloc[j]))].groupby(
                        ['date'])['volume'].sum())
                    for k in range(len(finals)):
                        finals.volume.iloc[k] = finals.volume.iloc[k] * float(eth[(finals.index[k] - start).days][4])
                    finals_inter = pd.concat([finals_inter, finals], axis=1).fillna(0).astype(int)
                    del finals
                
                elif clients.quote_id.iloc[i] == '34629b4b-753c-4537-865f-4b62ff1a31d6':
                    finals = pd.DataFrame(volumes[(volumes.currency==clients.currency_id.iloc[i])&
                       (volumes.quote==clients.quote_id.iloc[i])&
                       ((volumes.maker_trader==clients[clients.pair==clients.pair.iloc[i]].client_id.iloc[j])|
                       (volumes.taker_trader==clients[clients.pair==clients.pair.iloc[i]].client_id.iloc[j]))].groupby(
                        ['date'])['volume'].sum())
                    for k in range(len(finals)):
                        finals.volume.iloc[k] = finals.volume.iloc[k] * float(trx[(finals.index[k] - start).days][4])
                    finals_inter = pd.concat([finals_inter, finals], axis=1).fillna(0).astype(int)
                    del finals
            
            finals_inter = finals_inter.sum(axis=1) # Here we sum up all volume data from all accounts for the pair
            # Here we check whether there was no volume, then we don't add spreads as requirements have already been violated
            if finals_inter.empty:
                finals_inter = pd.DataFrame({'volume': [0], 'spread': [0], 'pair': [clients.pair.iloc[i]]
                                            }, index = pd.date_range(start, end, freq='D'))
            else: # If data is not empty we have to convert it to dataframe as Series has to join function
                finals_inter = pd.DataFrame(finals_inter)
                finals_inter = finals_inter.rename({0: 'volume'}, axis=1) # Renaming necessary, otherwise 
                # columns after joining at the next step will have the same names
                finals_inter = finals_inter.join(spreadsWeek[spreadsWeek[1]==clients.pair.iloc[i]]).rename(
                    {1: 'pair', 2: 'spread'}, axis=1).fillna(0)      
            
            final = final.append(finals_inter[['volume', 'pair', 'spread']])
            del finals_inter
            clients.loc[(clients.pair==clients.pair.iloc[i]), 'done'] = 1

    # At this step we check whether the project satisfied the requirements every day
    final = final.reset_index()
    final = final.rename({'index': 'date'}, axis=1)
    final['validation'] = 0
    for i in range(len(final)):
        if (final.volume.iloc[i] >= volume_needed) & (final.spread.iloc[i] <= spread_needed):
            final.loc[i, 'validation'] = 'OK'
        else:
            final.loc[i, 'validation'] = 'FAILED'

    # At this step we create a final report with a pair, status (ok/failed) and particular days when they failed, if any
    report = pd.DataFrame(columns = ['Pair', 'Validation', 'date', 'volume', 'spread'])
    for i in range(len(clients.pair.unique())):
        if final[(final.pair == clients.pair.unique()[i])&(final.validation == 'FAILED')].empty:
            intermediary = pd.DataFrame({'Pair': [clients.pair.unique()[i]], 
                                        'Validation': ['Satisfied']})
        else:
            intermediary = pd.DataFrame(columns = ['Pair', 'Validation', 'date', 'volume', 'spread'], index = [0])
            intermediary.Pair.iloc[0] = clients.pair.unique()[i]
            intermediary.Validation.iloc[0] = 'Failed on:'
            intermediary = intermediary.append(final[(final.pair == clients.pair.unique()[i])&(final.validation == 'FAILED')][[
                'date', 'spread', 'volume']], ignore_index=True).fillna(' ')
        report = report.append(intermediary, ignore_index=True)
        del intermediary

    report = report[report.Pair!='USDTUSDT']


    # Getting data on current and expired zero fee schemes
    current = zeroFeesCurrent()
    expired = zeroFeesExpired()

    # Changing currency ids to tags
    currencies = set(set(current[['base_currency_id', 
                                  'quote_currency_id'
                                 ]].values.ravel())|set(expired[['base_currency_id', 
                                                                 'quote_currency_id'
                                                                ]].values.ravel()))
    currencies.discard(None)
    cur_dict = get_cur_tags(tuple(currencies))
    cur_dict.index = cur_dict.id
    cur_dict = cur_dict.drop(columns=['id'])
    cur_dict = cur_dict.to_dict()['tag']

    current['base_currency_id'] = current.base_currency_id.map(cur_dict).fillna('All')
    current['quote_currency_id'] = current.quote_currency_id.map(cur_dict).fillna('All')
    expired['base_currency_id'] = expired.base_currency_id.map(cur_dict).fillna('All')
    expired['quote_currency_id'] = expired.quote_currency_id.map(cur_dict).fillna('All')

    # Saving the report
    writer = pd.ExcelWriter(f'{os.getcwd()}/PB_zero_fee_accounts_{start.isoformat()}_to_{end.isoformat()}.xlsx')
    current.to_excel(writer, sheet_name='Current ZFA', index = False)
    expired.to_excel(writer, sheet_name='Expired ZFA', index = False)
    report.to_excel(writer, sheet_name='Performance Based ZFA', index = False)
    writer.save()


    
        