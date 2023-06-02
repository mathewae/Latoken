import psycopg2

conn = psycopg2.connect(
    database="postgres",
    user="mandreev",
    password="ghAwqTy7jnKlozAyhNol7AQ6tgvZa129",
    host="dwh.nekotal.tech",
    port="5432"
)
#
# print("I am unable to connect to the database")

cur = conn.cursor()
commision_list=[]

select_Query = '''with t as (
    select
        cost * case
                    when maker_trader = '2b58cccc-5fbd-4154-9e73-a009d7c145c9' and direction = 'BUY' then 1
                    when maker_trader = '2b58cccc-5fbd-4154-9e73-a009d7c145c9' and direction = 'SELL' then -1
                    when taker_trader = '2b58cccc-5fbd-4154-9e73-a009d7c145c9' and direction = 'BUY' then -1
                    when taker_trader = '2b58cccc-5fbd-4154-9e73-a009d7c145c9' and direction = 'SELL' then 1
                end x, *
    from
        view_market_aggregator_trade
    /* время */
    where
        __update_datetime >= '2022-02-21  00:01:00' and
        __update_datetime <= '2022-02-22  00:01:00' and

        (maker_trader = '2b58cccc-5fbd-4154-9e73-a009d7c145c9' or taker_trader = '2b58cccc-5fbd-4154-9e73-a009d7c145c9') and
        /* Исключить self-trade  */
        maker_trader <> taker_trader and

        /* Задать базовую валюту  */
        currency = (select id from view_asset_manager_currency_latest where tag = 'BTC') and

        /* Задать квотовую валюту */
        quote = (select id from view_asset_manager_currency_latest where tag = 'USDT')

    /* Сортировка по возрастанию времени */
    order by
        __update_datetime
) select
    (select tag from view_asset_manager_currency_latest where id = currency) as currency,
    (select tag from view_asset_manager_currency_latest where id = quote) as quote,
    case
        when maker_trader = '2b58cccc-5fbd-4154-9e73-a009d7c145c9' then direction
        when maker_trader <> '2b58cccc-5fbd-4154-9e73-a009d7c145c9' and direction = 'BUY' then 'SELL'
        when maker_trader <> '2b58cccc-5fbd-4154-9e73-a009d7c145c9' and direction = 'SELL' then 'BUY'
    end as L_direction,
    __update_datetime,
    round(price, 8) as price,
    round(quantity, 8) as quantity,
    round(cost,8) as  cost_of_trade,
    round(x,8) as PnL_change,
    round(sum(x) over (order by __update_datetime rows between unbounded PRECEDING and current row ), 8) as PnL_result
from t'''

cur.execute(select_Query)

rows_tuple = cur.fetchall()
rows = [list(elem) for elem in rows_tuple]

HEDGE_BUFFER = 300
COMMISIONS = 0.00075
SPREAD = 0.0002

commisions_sum = 0.0
PnL_sum = 0.0

j = 0
i = 0

running_sum = 0
running_q_sum = 0
trade_pnl = 0.0
l = len(rows[0])
i = l

print("Initial Trades History\n\n")

for k in range(len(rows)):
    for p in range(len(rows[0])):
        print((rows[k][p]), end=' | ')
    print("\n")

print("Trades History with Hedge trades\n")

print("Cur | Quote| B/S | Date                      | Price          | Quantity    | Cost        | Exposure Change   | Exposure Result ")
while j < len(rows)-1:

    #calculating running amount
    match rows[j][2]:
        case "SELL": running_q_sum -= rows[j][i - 4]
        case "BUY": running_q_sum += rows[j][i - 4]

    # calculating running Profit & Loss
    match rows[j][2]:
        case "SELL": trade_pnl += float(rows[j][i - 4]) * float(rows[j][i - 5])
        case "BUY": trade_pnl -= float(rows[j][i - 4]) * float(rows[j][i - 5])

    running_sum += rows[j][l - 2]
    rows[j][l - 1] = running_sum

    if rows[j][l - 1] >= HEDGE_BUFFER:
        hedge_trade = rows[j].copy()
        rows.insert(j + 1, hedge_trade)
        rows[j + 1][0] = "HEDGE"
        rows[j + 1][l - 4] = running_q_sum
        rows[j + 1][l - 3] = - running_sum
        rows[j + 1][l - 2] = - running_sum
        rows[j + 1][l - 1] = 0

        if running_q_sum < 0:
            rows[j + 1][2] = "BUY"
            trade_pnl -= float(rows[j + 1][i - 4]) * float(rows[j + 1][i - 5]) * (1 - SPREAD/2)
            PnL_sum += trade_pnl
            trade_pnl = 0
        else:
            rows[j + 1][2] = "SELL"
            trade_pnl += float(rows[j + 1][i - 4]) * float(rows[j + 1][i - 5]) * (1 + SPREAD/2)
            PnL_sum += trade_pnl
            trade_pnl = 0

        running_q_sum = 0
        running_sum = 0
        commisions_sum += abs(float(rows[j + 1][l - 3]) * COMMISIONS)
        commision_list.append(abs(float(rows[j + 1][l - 3]) ))
        j +=2

    elif rows[j][l - 1] <= -HEDGE_BUFFER:
        hedge_trade = rows[j].copy()
        rows.insert(j + 1, hedge_trade)
        rows[j + 1][0] = "HEDGE"
        rows[j + 1][l - 4] = - running_q_sum
        rows[j + 1][l - 3] = - running_sum
        rows[j + 1][l - 2] = - running_sum
        rows[j + 1][l - 1] = 0

        if running_q_sum < 0:
            rows[j + 1][2] = "BUY"
            trade_pnl -= float(rows[j + 1][i - 4]) * float(rows[j + 1][i - 5]) * (1 - SPREAD/2)
            PnL_sum += trade_pnl
            trade_pnl = 0
        else:
            rows[j + 1][2] = "SELL"
            trade_pnl += float(rows[j + 1][i - 4]) * float(rows[j + 1][i - 5]) * (1 + SPREAD/2)
            PnL_sum += trade_pnl
            trade_pnl = 0

        running_q_sum = 0
        running_sum = 0
        commisions_sum += abs(float(rows[j + 1][l - 3]) * COMMISIONS)
        commision_list.append(abs(float(rows[j + 1][l - 3])))
        j += 2

    else:
        j += 1

#The last row

length = len(rows) -1
match rows[length][2]:
    case "SELL": running_q_sum -= rows[length][i - 4]
    case "BUY": running_q_sum += rows[length][i - 4]

# calculating running Profit & Loss
match rows[length][2]:
    case "SELL": trade_pnl += float(rows[length][i - 4]) * float(rows[length][i - 5])
    case "BUY": trade_pnl -= float(rows[length][i - 4]) * float(rows[length][i - 5])

running_sum += rows[length][l - 2]
rows[length][l - 1] = running_sum

if rows[length][l - 1] >= HEDGE_BUFFER:
    hedge_trade = rows[l].copy()
    rows.append(hedge_trade)
    rows[length + 1][0] = "HEDGE"
    rows[length + 1][l - 4] = running_q_sum
    rows[length + 1][l - 3] = - running_sum
    rows[length + 1][l - 2] = - running_sum
    rows[length + 1][l - 1] = 0

    if running_q_sum < 0:
        rows[length + 1][2] = "BUY"
        trade_pnl -= float(rows[length + 1][i - 4]) * float(rows[length + 1][i - 5]) * (1 - SPREAD/2)
        PnL_sum += trade_pnl
        trade_pnl = 0
    else:
        rows[length + 1][2] = "SELL"
        trade_pnl += float(rows[length + 1][i - 4]) * float(rows[length + 1][i - 5]) * (1 + SPREAD/2)
        PnL_sum += trade_pnl
        trade_pnl = 0

    running_q_sum = 0
    running_sum = 0
    commisions_sum += abs(float(rows[length + 1][l - 3]) * COMMISIONS)
    j += 2

elif rows[length][l - 1] <= -HEDGE_BUFFER:
    hedge_trade = rows[length].copy()
    rows.append(hedge_trade)
    rows[length + 1][0] = "HEDGE"
    rows[length + 1][l - 4] = - running_q_sum
    rows[length + 1][l - 3] = - running_sum
    rows[length + 1][l - 2] = - running_sum
    rows[length + 1][l - 1] = 0

    if running_q_sum < 0:
        rows[length + 1][2] = "BUY"
        trade_pnl -= float(rows[length + 1][i - 4]) * float(rows[length + 1][i - 5]) * (1 - SPREAD/2)
        PnL_sum += trade_pnl
        trade_pnl = 0
    else:
        rows[length + 1][2] = "SELL"
        trade_pnl += float(rows[length + 1][i - 4]) * float(rows[length + 1][i - 5]) * (1 + SPREAD/2)
        PnL_sum += trade_pnl
        trade_pnl = 0

    running_q_sum = 0
    running_sum = 0
    commisions_sum += abs(float(rows[length + 1][l - 3]) * COMMISIONS)
    j += 2


else:
    j += 1

    # match j:
    #     case 0: print(("{:>20}".formatrows[j][i]), end=' | ')

for k in range(len(rows)):
    for p in range(len(rows[0])):
        print((rows[k][p]), end=' | ')
    print("\n\n")


print("Total PnL Result ", PnL_sum, " $")
print("Total Commiions payed on Hedge ", commisions_sum , " $")
cur.close()

conn.close()

