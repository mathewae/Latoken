def build_query_binance_spot_trades(tz_diff: int, start_utc: str, end_utc: str):
    return f'''
        select
            order_trade_time + interval {tz_diff} hour as timestamp,
            symbol                    as pair,
            last_currency_qty as t_cur_amount,
            last_quote_qty as b_cur_amount,
            side,
            'Binance' as trader,
            is_maker
        from binance_spot_trades
        where order_trade_time between '{start_utc}' and '{end_utc}'
    '''


def build_query_gateio_spot_trades(tz_diff: int, start_utc: str, end_utc: str):
    return f'''
    select created as timestamp,
       symbol                    as pair,
       base_amount               as t_cur_amount,
       toFloat64(base_amount) * toFloat64(price)       as b_cur_amount,
       side,
       'GateIO'                  as trader,
       case
           when execution_type = 'MAKER' then 1
           else 0
           end                      is_maker
    from account_trade_gateio
    where created between '{start_utc}' and '{end_utc}'
    '''


def build_query_usd_prices_for_period(start_utc, end_utc: str):
    return f'''
        select asset,
               sum(total_rfc) / sum(total) as usd_price
        from bot_trading_balances_arch as t1
        right join (
            select max(datetime) as max_datetime, asset
            from bot_trading_balances_arch
            where datetime >= '{start_utc}' and datetime <= '{end_utc}' 
              and total > 0
              and total_rfc > 0
            group by asset
        ) as t2
        on t1.datetime = t2.max_datetime and t1.asset = t2.asset
        where datetime >= '{start_utc}' and datetime <= '{end_utc}'
        group by asset
    '''


def build_query_all_pairs_combinations():
    return f'''
        select concat(tags1.tag, tags2.tag) as pair,
               tags1.tag as t_cur,
               tags2.tag as b_cur
        from view_asset_manager_currency_latest as tags1,
             view_asset_manager_currency_latest as tags2
    '''


def build_query_all_pairs():
    return f'''
        select 
            concat(vamc_base.tag, vamc_quote.tag) pair, 
            vamc_base.tag t_cur, 
            vamc_quote.tag b_cur
            from view_asset_manager_pair pairs
            join view_asset_manager_currency vamc_base on pairs.currency_id = vamc_base.id
            join view_asset_manager_currency vamc_quote on pairs.quote_id = vamc_quote.id
    '''


def build_query_latoken_trades(tz_diff: int, start_utc: str, end_utc: str):
    return f'''
        select timestamp,
               trader,
               is_maker,
               direction,
               concat(t_cur.tag, b_cur.tag) as pair,
               t_cur.tag as t_cur,
               b_cur.tag as b_cur,
               quantity as t_cur_amount,
               cost as b_cur_amount
        from (
            select __update_datetime + interval '{tz_diff} hours' as timestamp,
                   quote,
                   currency,
                   quantity,
                   cost,
                   maker_trader as trader,
                   1 as is_maker,
                   direction
            from view_market_aggregator_trade
            where __update_datetime between '{start_utc}' and '{end_utc}'
              and maker_trader in (
                    '2b58cccc-5fbd-4154-9e73-a009d7c145c9',
                    '8fdd328f-47cc-4705-bf45-9a4f20314b87',
                    'ddee6a46-3bbd-4d39-b561-c028e618d8ff'
              )
              and taker_trader <> maker_trader
            union all
            select __update_datetime + interval '{tz_diff} hours' as timestamp,
                   quote,
                   currency,
                   quantity,
                   cost,
                   taker_trader as trader,
                   0 as is_maker,
                   direction
            from view_market_aggregator_trade
            where __update_datetime between '{start_utc}' and '{end_utc}'
              and taker_trader in (
                    '2b58cccc-5fbd-4154-9e73-a009d7c145c9',
                    '8fdd328f-47cc-4705-bf45-9a4f20314b87',
                    'ddee6a46-3bbd-4d39-b561-c028e618d8ff'
              )
              and taker_trader <> maker_trader
            ) as trades
        left join view_asset_manager_currency_latest as b_cur
            on b_cur.id = trades.quote
        left join view_asset_manager_currency_latest as t_cur
            on t_cur.id = trades.currency
    '''
