from datetime import datetime, timedelta
import pandas
import utils
import query_builders
import databases

# commented timezone dependency
# TZ_DIFF = datetime.now().hour - datetime.utcnow().hour
TZ_DIFF: int = datetime.utcnow().hour - datetime.utcnow().hour
START_UTC, END_UTC = utils.get_period_utc(TZ_DIFF)
TRADES_COL_ORDER = ['timestamp', 'trader', 'is_maker', 'side', 'pair', 't_cur', 'b_cur', 't_cur_amount', 'b_cur_amount']


def export_df_to_excel(df, writer, sheet_name: str, datetime_format: str):
    def get_series_max_len(series, datetime_format):
        if series.dtype == 'datetime64[ns]':
            return len(datetime_format)
        if series.dtype == 'float64'    :
            return len(str(round(series.max(), 4)))
        return df[column].astype(str).str.len().max()

    df.to_excel(writer, sheet_name=sheet_name, index=False)
    for i, column in enumerate(df.columns):
        column_len = max(len(column), get_series_max_len(df[column], datetime_format))
        writer.sheets[sheet_name].set_column(i, i, column_len)


query_binance_spot_trades = databases.clickhouse_query_executor_decorator(
    query_builders.build_query_binance_spot_trades
)
query_gateio_spot_trades = databases.clickhouse_query_executor_decorator(
    query_builders.build_query_gateio_spot_trades
)
query_usd_prices_for_period = databases.clickhouse_query_executor_decorator(
    query_builders.build_query_usd_prices_for_period
)
query_all_pairs = databases.postgres_query_executor_decorator(
    query_builders.build_query_all_pairs
)
query_latoken_trades = databases.postgres_query_executor_decorator(
    query_builders.build_query_latoken_trades
)

pairs_report = utils.Params.get_param('pairs', single=False, required=False, value_if_missing='All_pairs')
usd_prices = query_usd_prices_for_period(START_UTC, END_UTC)
# usd_prices = query_usd_prices_for_period(END_UTC - timedelta(hours=1), END_UTC)

trades_binance = pandas.merge(
    query_binance_spot_trades(TZ_DIFF, START_UTC, END_UTC),
    query_all_pairs(),
    on='pair',
    how='left'
)
trades_binance['timestamp'] = pandas.to_datetime(trades_binance['timestamp'])

trades_gateio = pandas.merge(
    query_gateio_spot_trades(TZ_DIFF, START_UTC, END_UTC),
    query_all_pairs(),
    on='pair',
    how='left'
)
trades_gateio['timestamp'] = pandas.to_datetime(trades_gateio['timestamp'])

trades_latoken = query_latoken_trades(TZ_DIFF, START_UTC, END_UTC)

for i in range(len(trades_latoken)):
    direction = trades_latoken.at[i, 'direction']
    trades_latoken.at[i, 'side'] = direction if trades_latoken.at[i, 'is_maker'] == 1 else \
        ('SELL' if direction == 'BUY' else 'BUY')

trades_latoken.drop(columns='direction', inplace=True)

trades = pandas.merge(
    pandas.concat([trades_binance[TRADES_COL_ORDER], trades_latoken[TRADES_COL_ORDER], trades_gateio[TRADES_COL_ORDER]], ignore_index=True),
    usd_prices.rename(
        columns={
            'asset': 'b_cur',
            'usd_price': 'fx_rate_base'
        }
    ),
    on='b_cur',
    how='left'
)

if pairs_report != 'All_pairs':
    trades = trades.loc[trades['pair'].isin(pairs_report)]
pairs_report = trades.filter(items=['pair', 't_cur', 'b_cur']).drop_duplicates()
pairs_report.reset_index(drop=True, inplace=True)

trades.sort_values(by='timestamp', inplace=True)
trades.reset_index(drop=True, inplace=True)

# добавление расчетных столбцов, приведение операций к итоговому виду
trades['volume'] = trades['b_cur_amount'] * trades['fx_rate_base']
for i in range(len(trades)):
    trades.at[i, 't_cur_amount_buy'] = trades.at[i, 't_cur_amount'] if trades.at[i, 'side'] == 'BUY' else 0
    trades.at[i, 't_cur_amount_sell'] = trades.at[i, 't_cur_amount'] * (-1) if trades.at[i, 'side'] == 'SELL' else 0
    trades.at[i, 'b_cur_amount_buy'] = 0 if trades.at[i, 'side'] == 'BUY' else trades.at[i, 'b_cur_amount']
    trades.at[i, 'b_cur_amount_sell'] = 0 if trades.at[i, 'side'] == 'SELL' else trades.at[i, 'b_cur_amount'] * (-1)

trades['t_cur_amount'] = trades['t_cur_amount_buy'] + trades['t_cur_amount_sell']
trades['b_cur_amount'] = trades['b_cur_amount_buy'] + trades['b_cur_amount_sell']

trades_to_merge = trades.filter(
    items=[
        'pair',
        't_cur_amount',
        'b_cur_amount',
        't_cur_amount_buy',
        't_cur_amount_sell',
        'b_cur_amount_buy',
        'b_cur_amount_sell'
    ]
).rename(columns={
    't_cur_amount': 'Amount_chg_t_cur',
    'b_cur_amount': 'Amount_chg_b_cur'
})
trades.drop(columns=['t_cur_amount_buy', 't_cur_amount_sell', 'b_cur_amount_buy', 'b_cur_amount_sell'], inplace=True)
trades['price'] = trades['b_cur_amount'] / trades['t_cur_amount'] * -1

if len(pairs_report) == 1:
    total_binance, total_latoken, total_gateio = 0, 0, 0
    for i in range(len(trades)):
        if trades.at[i, 'trader'] == 'Binance':
            total_binance += trades.at[i, 't_cur_amount']
        elif trades.at[i, 'trader'] == 'GateIO':
            total_gateio += trades.at[i, 't_cur_amount']
        else:
            total_latoken += trades.at[i, 't_cur_amount']
        trades.at[i, 'binance'], trades.at[i, 'latoken'], trades.at[i, 'gateio'] = total_binance, total_latoken, total_gateio
    trades['total_pos_chg'] = trades['binance'] + trades['latoken'] + trades['gateio']

# компиляция отчета
report = pandas.merge(pairs_report, usd_prices.rename(columns={'asset': 't_cur', 'usd_price': 't_cur_usd_price'}),
                      on='t_cur', how='left')
report = pandas.merge(report, usd_prices.rename(columns={'asset': 'b_cur', 'usd_price': 'b_cur_usd_price'}),
                      on='b_cur', how='left')
report = pandas.merge(report, trades_to_merge.groupby(by='pair').sum(), on='pair')
report['Amount_chg_t_cur_USD'] = report['Amount_chg_t_cur'] * report['t_cur_usd_price']
report['Amount_chg_b_cur_USD'] = report['Amount_chg_b_cur'] * report['b_cur_usd_price']
report['total_result'] = report['Amount_chg_t_cur_USD'] + report['Amount_chg_b_cur_USD']
if abs(max(trades['total_pos_chg'])) > abs(min(trades['total_pos_chg'])):
    report['max_exposure'] = (max(trades['total_pos_chg']))
else:
    report['max_exposure'] = (min(trades['total_pos_chg']))
report['Avg_buy_price'] = abs(sum(report['b_cur_amount_sell']) / sum(report['t_cur_amount_buy']))
report['Avg_sell_price'] = abs(sum(report['b_cur_amount_buy']) / sum(report['t_cur_amount_sell']))
report['avg_price_diff_b_cur_%'] = round((report['t_cur_amount_buy'] * report['b_cur_amount_buy']
                                          / report['t_cur_amount_sell'] / report['b_cur_amount_sell'] - 1) * 100, 1)
report.drop(columns=['t_cur_amount_buy', 't_cur_amount_sell', 'b_cur_amount_buy', 'b_cur_amount_sell'], inplace=True)

report['total_result_abs'] = abs(report['total_result'])
report.sort_values(by='total_result_abs', ascending=False, inplace=True)
report.drop(columns='total_result_abs', inplace=True)

# выгрузка в Excel
datetime_format = 'hh:mm:ss.000'
with pandas.ExcelWriter(
        f'reports/PL_incidents_report_{pairs_report.at[0, "pair"]}_{str(datetime.now().replace(microsecond=0)).replace(":", "-")}.xlsx',
        engine='xlsxwriter',
        datetime_format=datetime_format
) as writer:
    export_df_to_excel(report, writer, 'Trading_PL', datetime_format)
    export_df_to_excel(trades, writer, 'Trades', datetime_format)

    if len(pairs_report) == 1:
        col_count = len(trades.columns)
        writer.sheets['Trades'].set_column(col_count, col_count, 3)

        chart = writer.book.add_chart({'type': 'line'})
        last_row = len(trades.index)
        chart.add_series({'categories': f'=Trades!$A$2:$A${last_row}', 'values': f'=Trades!$P$2:$P${last_row}'})
        chart.set_legend({'position': 'none'})
        chart.set_title({'name': f'{pairs_report.at[0, "pair"]} position change'})
        writer.sheets['Trades'].insert_chart('Q2', chart)
