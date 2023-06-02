import psycopg2
import requests
import pandas as pd
from datetime import date, datetime, timezone, timedelta
import os
from credentials import dwh_user, dwh_pass
from time import sleep

# Inputs
start = date(2021, 7, 19)
end = date(2021, 7, 25)

# Define functions
def run_dwh_sql(sql_query):
    conn = psycopg2.connect(host='135.181.61.116', user=dwh_user, password=dwh_pass, dbname='postgres',port='5432')
    data = pd.read_sql(sql_query, conn)
    conn.close()
    return data

def get_trades(start_date, end_date):
    sql_query = f"""
    select __create_date as date, direction, currency, quote, sum(cost) as cost, sum(fee) as fees, sum(quantity) as quantity, sum(price*quantity)/sum(quantity) as price, status
    from (select __create_date, direction, quantity, currency, quote, cost, maker_fee as fee, price, 'maker' as status
        from view_market_aggregator_trade
        where maker_trader = '41f1c537-2a69-4dc7-88d3-560d76eb278b' 
        and __update_datetime >= '{start_date} 00:00:00' and __update_datetime <= '{end_date} 23:59:59'
        union all
        select __create_date, direction, quantity, currency, quote, cost, taker_fee as fee, price, 'taker' as status
        from view_market_aggregator_trade
        where taker_trader = '41f1c537-2a69-4dc7-88d3-560d76eb278b' 
        and __update_datetime >= '{start_date} 00:00:00' and __update_datetime <= '{end_date} 23:59:59') w
    group by __create_date, currency, quote, direction, status
    order by __create_date, currency, quote, direction, status;
    """ 
    return run_dwh_sql(sql_query)

def get_cur_ids(ids):
    sql_query = f"""
    select id, tag, name
    from view_asset_manager_currency
    where id in {ids};
    """ 
    return run_dwh_sql(sql_query)

# Running the main part
if __name__ == '__main__':
    # Getting trades
    trades = get_trades(start, end)

    # Correct trade direction
    for i in range(len(trades)):
        if trades.status.iloc[i] == 'taker':
            if trades.direction.iloc[i]=='BUY':
                trades.loc[i, 'direction']='SELL'
            else:
                trades.loc[i, 'direction']='BUY'

    # Getting currency and quote ids
    unique_currencies = tuple(pd.unique(trades[['currency', 'quote']].values.ravel()))
    currency_ids = get_cur_ids(unique_currencies)
    currency_ids_dict = pd.Series(currency_ids.tag.values, index=currency_ids.id).to_dict()
    currency_name_dict = pd.Series(currency_ids.name.values, index=currency_ids.tag).to_dict()
    trades['currency_tag'] = trades.currency.map(currency_ids_dict)
    trades['quote_tag'] = trades.quote.map(currency_ids_dict)
    trades['pair'] = trades['currency_tag'] + '/' + trades['quote_tag']


    # This section: Binance prices
    # Getting and assigning correct exchange rate to convert to usd
    start_bin = int(str(int(datetime(start.year, start.month, start.day, 0, 0).replace(tzinfo=timezone.utc).timestamp()))+'000')
    end_bin = int(str(int(datetime(end.year, end.month, end.day, 0, 0).replace(tzinfo=timezone.utc).timestamp()))+'000')
    btc = requests.get(f"https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1d&startTime={start_bin}&endTime={end_bin}").json()
    eth = requests.get(f"https://api.binance.com/api/v3/klines?symbol=ETHUSDT&interval=1d&startTime={start_bin}&endTime={end_bin}").json()
    trx = requests.get(f"https://api.binance.com/api/v3/klines?symbol=TRXUSDT&interval=1d&startTime={start_bin}&endTime={end_bin}").json()
    eos = requests.get(f"https://api.binance.com/api/v3/klines?symbol=EOSUSDT&interval=1d&startTime={start_bin}&endTime={end_bin}").json()

    trades['rate'] = 0
    for j in range(len(trades)):
        if trades.quote.iloc[j] == '0c3a106d-bde3-4c13-a26e-3fd2394529e5':  #USDT
            trades.loc[j, 'rate'] = 1
        elif trades.quote.iloc[j] == '707ccdf1-af98-4e09-95fc-e685ed0ae4c6':  #LA
            trades.loc[j, 'rate'] = 0.04
        elif trades.quote.iloc[j] == '92151d82-df98-4d88-9a4d-284fa9eca49f':  #BTC
            trades.loc[j, 'rate'] = (float(btc[(trades.date.iloc[j] - start).days][2]) + float(btc[(trades.date.iloc[j] - start).days][3]))/2
        elif trades.quote.iloc[j] == '620f2019-33c0-423b-8a9d-cde4d7f8ef7f':  #ETH
            trades.loc[j, 'rate'] = (float(eth[(trades.date.iloc[j] - start).days][2]) + float(eth[(trades.date.iloc[j] - start).days][3]))/2
        elif trades.quote.iloc[j] == '34629b4b-753c-4537-865f-4b62ff1a31d6':  #TRX
            trades.loc[j, 'rate'] = (float(trx[(trades.date.iloc[j] - start).days][2]) + float(trx[(trades.date.iloc[j] - start).days][3]))/2
        elif trades.quote.iloc[j] == 'd286007b-03eb-454e-936f-296c4c6e3be9':
            trades.loc[j, 'rate'] = (float(eos[(trades.date.iloc[j] - start).days][2]) + float(eos[(trades.date.iloc[j] - start).days][3]))/2

    # Stylying
    trades = trades.drop(columns = ['currency', 'quote', 'status'])
    trades = trades[['date', 'currency_tag', 'quote_tag', 'pair', 'quantity', 'price', 'cost', 'fees', 'direction', 'rate']]


    # This section: Coingecko
    # Getting unique currencies/tickers from trades list
    curs = trades.currency_tag.unique()
    # Getting all available tokens from coingecko
    tokens = requests.get('http://api.coingecko.com/api/v3/coins/list').json()

    # Getting all ids from Coingecko
    geetingAll = list() # List for coingecko id, name, ticker for necessary projects
    unknown = list() # List for tokens not on coingecko
    alreadyAll = list() # List that writes down which tickers and how many of each were found on coingecko
    for i in range(len(curs)):
        for j in range(len(tokens)):
            if tokens[j]['symbol'].lower() == curs[i].lower():
                geetingAll.append(tokens[j])
                alreadyAll.append(curs[i])
        if curs[i] not in alreadyAll:
            unknown.append(curs[i])

    # Getting prices by ids from Coingecko 
    start_cg = int(str(int(datetime(start.year, start.month, start.day, 0, 0).replace(tzinfo=timezone.utc).timestamp())))
    end_cg = int(str(int((datetime(end.year, end.month, end.day, 0, 0) + timedelta(days=1)).replace(tzinfo=timezone.utc).timestamp())))

    trades['coingecko'] = 0
    for i in range(len(alreadyAll)):
        sleep(0.5)
        if alreadyAll.count(alreadyAll[i]) == 0:
            print(f'Something weird, project {i} has no symbol though on the list')
        elif alreadyAll.count(alreadyAll[i]) == 1:
            try:
                prices = requests.get(f"https://api.coingecko.com/api/v3/coins/{geetingAll[i]['id']}/market_chart/range?vs_currency=usd&from={start_cg}&to={end_cg}").json()['prices']
            except: 
                print('There was a problem')
                print(requests.get(f"https://api.coingecko.com/api/v3/coins/{geetingAll[i]['id']}/market_chart/range?vs_currency=usd&from={start_cg}&to={end_cg}").json())
            for j in range(len(trades)):
                if trades.currency_tag.iloc[j]==alreadyAll[i]:
                    barePrices = list()
                    for k in range(len(prices[24*(trades.date.iloc[j]-start).days:24*((trades.date.iloc[j]-start).days+1)])):
                        barePrices.append(prices[24*(trades.date.iloc[j]-start).days:24*((trades.date.iloc[j]-start).days+1)][k][1])
                    try:
                        trades.loc[j, 'coingecko'] = sum(barePrices)/len(barePrices)
                        print(f'Done {alreadyAll[i]} for {trades.date.iloc[j]} with price {sum(barePrices)/len(barePrices)}')
                    except:
                        print(f'Problem for {trades.currency_tag.iloc[j]} on {trades.date.iloc[j]}')
                else:
                    continue
        elif alreadyAll.count(alreadyAll[i]) > 1:
            buffer = geetingAll[i:i+alreadyAll.count(alreadyAll[i])]
            actualName = currency_name_dict[alreadyAll[i]]
            for k in range(len(buffer)):
                checker = buffer[k]['name'].lower().find(actualName.lower())
                if checker != -1:
                    prices = requests.get(f"https://api.coingecko.com/api/v3/coins/{buffer[k]['id']}/market_chart/range?vs_currency=usd&from={start_cg}&to={end_cg}").json()['prices']
                    for j in range(len(trades)):
                        if trades.currency_tag.iloc[j]==alreadyAll[i]:
                            barePrices = list()
                            for k in range(len(prices[24*(trades.date.iloc[j]-start).days:24*((trades.date.iloc[j]-start).days+1)])):
                                barePrices.append(prices[24*(trades.date.iloc[j]-start).days:24*((trades.date.iloc[j]-start).days+1)][k][1])
                            try:
                                trades.loc[j, 'coingecko'] = sum(barePrices)/len(barePrices)
                                print(f'Done {alreadyAll[i]} for {trades.date.iloc[j]} with price {sum(barePrices)/len(barePrices)}')
                            except:
                                print(f'Problem for {trades.currency_tag.iloc[j]} on {trades.date.iloc[j]}')

    # Adding daily prices from LATOKEN, if token not on Coingecko
    for j in range(len(trades)):
        if trades.coingecko.iloc[j]==0 or trades.currency_tag.iloc[j]=='TRADE':
            start_lat = int(datetime(trades.date.iloc[j].year, trades.date.iloc[j].month, 
                     trades.date.iloc[j].day, 0, 0).replace(tzinfo=timezone.utc).timestamp())

            end_lat = int(datetime(trades.date.iloc[j].year, trades.date.iloc[j].month, 
                     trades.date.iloc[j].day, 23, 59).replace(tzinfo=timezone.utc).timestamp())

            lat = requests.get(f'https://api.latoken.com/v2/tradingview/history?symbol={trades.currency_tag.iloc[j]}%2F{trades.quote_tag.iloc[j]}&resolution=1h&from={start_lat}&to={end_lat}').json()['c']

            total = list()
            for i in range(len(lat)):
                total.append(float(lat[i]))

            trades.loc[j, 'coingecko'] = sum(total)/len(total) * trades.rate.iloc[j]
            print(f"Done {trades.currency_tag.iloc[j]} for {trades.date.iloc[j]} with price {trades.loc[j, 'coingecko']}")
            del start_lat, end_lat, lat, total
                                
    # Calculating target and actual amounts
    trades['target'] = trades.coingecko * trades.quantity # Target
    trades['actual'] = trades.cost * trades.rate # Actual
    trades['deviation'] = trades['actual'] - trades['target']
    trades = trades.sort_values(by='deviation', ascending = True)

    # Saving
    writer = pd.ExcelWriter(f"{os.getcwd()}/realisation_audit_{start}_{end}.xlsx")
    trades.to_excel(writer, sheet_name='trades', index = False)
    trades[(trades.deviation>100)|(trades.deviation<-100)][['date', 'pair', 'target', 'actual', 'deviation']].round(2).to_excel(writer, sheet_name='outliers', index = False)
    writer.save()
