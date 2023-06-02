import psycopg2
import xlsxwriter
import datetime
import pandas

#Cration of excel sheet and setting of the format

workbook = xlsxwriter.Workbook(r"C:\Users\matve\LATOKEN_PY\Sample.xlsx")

# TOKENS_LIST = ['BTCUSDT']
# BUFFER_LIST = [20]
# START_DATE = datetime.datetime(2022, 1, 10)
# END_DATE = datetime.datetime(2022, 1, 11)


TOKENS_LIST = ['GARI']
BUFFER_LIST = [90,100]
START_DATE = datetime.datetime(2022, 2, 1)
END_DATE = datetime.datetime(2022, 3, 1)
DATE_LIST = pandas.date_range(min(START_DATE, END_DATE), max(START_DATE, END_DATE)).strftime('%Y-%m-%d').tolist()
HEADER_FORMAT = workbook.add_format({'bold': True, 'border': True})
BOTTOM_FORMAT = workbook.add_format({"bottom": True})
RIGHT_FORMAT = workbook.add_format({"right": True})
ANGLE_FORMAT = workbook.add_format({"right": True, "bottom": True})

HEDGE_BUFFER = 100
COMMISIONS = 0.00075

# Connection to the Database

conn = psycopg2.connect(
    database="postgres",
    user="mandreev",
    password="ghAwqTy7jnKlozAyhNol7AQ6tgvZa129",
    host="dwh.nekotal.tech",
    port="5432"
)

cur = conn.cursor()


for token in range(len(TOKENS_LIST)):

    ws1 = workbook.add_worksheet(TOKENS_LIST[token])

    ws1.write(0, 0, TOKENS_LIST[token]+" / PnL-Comm", HEADER_FORMAT)
    ws1.set_column('A:A', 22)

    for buf in range(len(BUFFER_LIST)):
        ws1.write(0, buf + 1, BUFFER_LIST[buf], HEADER_FORMAT)
        ws1.write(len(DATE_LIST), buf + 1, '', BOTTOM_FORMAT)

    for dat in range(len(DATE_LIST)):
        ws1.write(dat + 1, 0 ,DATE_LIST[dat], HEADER_FORMAT)
        if dat < len(DATE_LIST) - 1 :
            ws1.write(dat + 1, len(BUFFER_LIST), '',RIGHT_FORMAT)
        else:
            ws1.write(dat + 1, len(BUFFER_LIST), '', ANGLE_FORMAT)

    for buf in range(len(BUFFER_LIST)):
        for dat in range(len(DATE_LIST) - 1):

            #qery request

            select_Query = """with t as (
                select
                    cost * case direction when 'BUY' then 1 else -1 end as x, *
                from
                    view_market_aggregator_trade
                /* время */
                where
                    __update_datetime >= '""" + str(DATE_LIST[dat]) + """  00:01:00' and
                    __update_datetime <= '""" + str(DATE_LIST[dat+1]) + """  00:01:00' and
            
                    (maker_trader = '2b58cccc-5fbd-4154-9e73-a009d7c145c9' or taker_trader = '2b58cccc-5fbd-4154-9e73-a009d7c145c9') and
                    /* Исключить self-trade  */
                    maker_trader <> taker_trader and
            
                    /* Задать базовую валюту  */
                    currency = (select id from view_asset_manager_currency_latest where tag = '""" + str(BUFFER_LIST[buf]) + """') and
            
                    /* Задать квотовую валюту */
                    quote = (select id from view_asset_manager_currency_latest where tag = 'USDT')
            
                /* Сортировка по возрастанию времени */
                order by
                    __update_datetime
            ) select
                (select tag from view_asset_manager_currency_latest where id = currency) as currency,
                (select tag from view_asset_manager_currency_latest where id = quote) as quote,
                direction,
                __update_datetime,
                round(price, 8) as price,
                round(quantity, 8) as quantity,
                round(cost,8) as  cost_of_trade,
                round(x,8) as PnL_change,
                round(sum(x) over (order by __update_datetime rows between unbounded PRECEDING and current row ), 8) as PnL_result
            from t"""

            cur.execute(select_Query)

            rows_tuple = cur.fetchall()
            rows = [list(elem) for elem in rows_tuple]

            commisions_sum = 0.0
            j = 0
            i = 0

            running_sum = 0
            running_q_sum = 0
            l = len(rows[0])

            # print("Initial Trades History\n")
            #
            # for k in range(len(rows)):
            #     for p in range(len(rows[0])):
            #         print((rows[k][p]), end=' | ')
            #     print("\n")
            #
            # print("Trades History with Hedge trades\n")
            #
            #print("Cur | Quote| B/S | Date                      | Price          | Quantity    | Cost        | Exposure Change   | Exposure Result ")
            while j < len(rows)-1:

                match rows[j][2]:
                    case "SELL": running_q_sum -= rows[j][i - 4]
                    case "BUY": running_q_sum += rows[j][i - 4]

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
                    else:
                        rows[j + 1][2] = "SELL"

                    running_q_sum = 0
                    running_sum = 0
                    commisions_sum += float(rows[j + 1][l - 3]) * COMMISIONS
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
                    else:
                        rows[j + 1][2] = "SELL"

                    running_q_sum = 0
                    running_sum = 0
                    commisions_sum += abs(float(rows[j + 1][l - 3]) * COMMISIONS)
                    j += 2

                else:
                    j += 1

            #The last row

            length = len(rows) -1
            match rows[length][2]:
                case "SELL":
                    running_q_sum -= rows[length][i - 4]
                case "BUY":
                    running_q_sum += rows[length][i - 4]

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
                else:
                    rows[length + 1][2] = "SELL"

                running_q_sum = 0
                running_sum = 0
                commisions_sum += float(rows[length + 1][l - 3]) * COMMISIONS
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
                else:
                    rows[length + 1][2] = "SELL"

                running_q_sum = 0
                running_sum = 0
                commisions_sum += float(rows[length + 1][l - 3]) * COMMISIONS
                j += 2

            else:
                j += 1

                # match j:
                #     case 0: print(("{:>20}".formatrows[j][i]), end=' | ')

            # for k in range(len(rows)):
            #     for p in range(len(rows[0])):
            #         print((rows[k][p]), end=' | ')
            #     print("\n")
            #
            # print("Total PnL Result - ", " $")
            # print("Total Commiions payed on Hedge - ", commisions_sum , " $")

            ws1.write(dat + 1, buf + 1, commisions_sum)

            cur.close()

            conn.close()

workbook.close()








