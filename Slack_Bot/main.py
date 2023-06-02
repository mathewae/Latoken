import sqlalchemy as sa
import json
import numpy as np
from bs4 import BeautifulSoup
from time import sleep
from tqdm import tqdm
import pandas as pd
import requests
import psycopg2
from clickhouse_driver import Client


def to_list(name):
    convert = ' '.join([str(elem) for elem in name])
    convert = convert.replace('(', '')
    convert = convert.replace(',)', '')
    abc = round(float(convert))
    return abc


def send_slack_message(payload, webhook):
    """Send a Slack message to a channel via a webhook. 

    Args:
        payload (dict): Dictionary containing Slack message, i.e. {"text": "This is a test"}
        webhook (str): Full Slack webhook URL for your chosen channel. 

    Returns:
        HTTP response code, i.e. <Response [503]>
    """

    return requests.post(webhook, json.dumps(payload))


client = Client(host='95.217.178.73',
                database='default',
                user='',
                password='')

fb_usdt = client.execute(
        'select total_rfc '
        'from floating_bot_trading_balances_arch '
        'where asset  =\'USDT\' '
        '   and account_type = \'SPOT\' '
        '   and investor_id = \'FLOATINGBOT\' '
        '   and date = today() order by datetime desc limit 1')
fb_usdt_balance = to_list(fb_usdt)

fbgol_usdt = client.execute(
        '''select total_rfc 
            from bot_trading_balances_arch
            where asset  =\'USDT\' 
            and account_type = \'SPOT\' 
            and investor_id = \'FLOATING-UNH\'
            and date = today()
            order by datetime desc limit 1''')
fbgol_usdt_balance = to_list(fbgol_usdt)

fbhype_usdt = client.execute(
        '''select total_rfc 
            from floating_bot_trading_balances_arch
            where asset  =\'USDT\' 
            and account_type = \'SPOT\'     
            and investor_id = \'FLOATINGBOT-HYPECOINS\'
            and date = today()
            order by datetime desc limit 1''')
fbhype_usdt_balance = to_list(fbhype_usdt)

fb_eth = client.execute(
        '''select total_rfc 
            from floating_bot_trading_balances_arch
            where asset  =\'ETH\' 
            and account_type = \'SPOT\'
            and investor_id = \'FLOATINGBOT\'
            and date = today()
            order by datetime desc limit 1''')
fb_eth_balance = to_list(fb_eth)

fb_btc = client.execute(
        '''select total_rfc 
            from floating_bot_trading_balances_arch
            where asset  =\'BTC\' 
            and account_type = \'SPOT\' 
            and investor_id = \'FLOATINGBOT\'
            and date = today()
            order by datetime desc limit 1''')
fb_btc_balance = to_list(fb_btc)

fb_trx = client.execute(
        '''select total_rfc 
            from floating_bot_trading_balances_arch
            where asset  =\'TRX\'
            and account_type = \'SPOT\'     
            and investor_id = \'FLOATINGBOT\'
            and date = today()
            order by datetime desc limit 1''')
fb_trx_balance = to_list(fb_trx)

alert = []

if fb_trx_balance < 100:
    fb_trx_balance_str = f'We have less than 100$(current = {str(fb_trx_balance)}) in TRX on FLOATINGBOT account \n'
    alert.append(fb_trx_balance_str)

if fb_btc_balance < 100:
    fb_btc_balance_str = f'We have less than 100$(current = {str(fb_btc_balance)})  in BTC on FLOATINGBOT account \n'
    alert.append(fb_btc_balance_str)

if fb_usdt_balance < 100:
    fb_usdt_balance_str = f'We have less than 100$(current = {str(fb_usdt_balance)})  in USDT on FLOATINGBOT account \n'
    alert.append(fb_usdt_balance_str)

if fb_eth_balance < 100:
    fb_eth_balance_str = f'We have less than 100$(current = {str(fb_eth_balance)})  in ETH on FLOATINGBOT account \n'
    alert.append(fb_eth_balance_str)

if fbgol_usdt_balance < 100:
    fbgol_usdt_balance_str = f'We have less than 100$(current = {str(fbgol_usdt_balance)})  in USDT on FLOATING-UNH(GOL) account \n'
    alert.append(fbgol_usdt_balance_str)

if fbhype_usdt_balance < 100:
    fbhype_usdt_balance_str = f'We have less than 100$(current = {str(fbhype_usdt_balance)})  in USDT on FLOATING-Hype account \n'
    alert.append(fbhype_usdt_balance_str)

str1 = ''.join(alert)
webhook = "https://hooks.slack.com/services/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"
print(str1)
# payload = {"text": "<!channel> Problem with FB quote balances :\n" + str1}
# send_slack_message(payload, webhook)