import datetime
import psycopg2
import xlsxwriter
import datetime
import pandas

TOKENS_LIST = ['BTC']
BUFFER_LIST = [20]
START_DATE = datetime.datetime(2022, 1, 10)
END_DATE = datetime.datetime(2022, 1, 12)
DATE_LIST = pandas.date_range(min(START_DATE, END_DATE), max(START_DATE, END_DATE)).strftime('%Y-%m-%d').tolist()

for token in range(len(TOKENS_LIST)):
    for buf in range(len(BUFFER_LIST)):
        for dat in range(len(DATE_LIST) - 1):
            # qery request

            print("""currency = (select id from view_asset_manager_currency_latest where tag = '""" + str(TOKENS_LIST[token]) + """')""")


