#!/usr/bin/env python
import pandas as pd
import numpy as np 
from core.data_processor import Dataloader
import json
import os
import time

print('处理中...请稍后...')
with open(os.path.join('config','colume_trade')) as f:
    colume_trade = f.read().split('\n')
with open(os.path.join('config','colume_position')) as f:
    colume_position = f.read().split('\n')
with open(os.path.join('config','colume_balance')) as f:
    colume_balance = f.read().split('\n')
with open(os.path.join('config','config.json'),encoding = 'utf-8') as f:
    config = json.load(f)
with open(os.path.join('config','table.json'),encoding = 'utf-8') as f:
    table = json.load(f)
with open(os.path.join('config','translation.json'),encoding = 'utf-8') as f:
    trans = json.load(f)
with open(os.path.join('config','contract.json'),encoding = 'utf-8') as f:
    contract = json.load(f)

trade_date_df = pd.read_csv(os.path.join('config','Last Trading Date.csv'))
trade_date_dict = dict(zip(trade_date_df['Trade Date'],trade_date_df['Last Trading Date']))


data = Dataloader(colume_trade,colume_position,colume_balance)
data.get_all_df(organize_table=table['table']['organize'],
    ctp_table=table['table']['ctp'],
    organize_path=table['path']['organize'],
    ctp_path=table['path']['ctp'],
    replace=table['replace'])

trade_data_df,position_data_df,balance_data_df = data.get_common_data(config,trans)
trade_data_df = data.get_trade_data(trade_data_df,contract,trans)
position_data_df = data.get_position_data(position_data_df,contract,trans,trade_date_dict)
balance_data_df = data.get_balance_data(balance_data_df)
#写入csv文件
date = time.strftime('%Y%m%d')
trade_data_df.to_csv(os.path.join('csv','SDIC_com_trade_t1_f__' + date + '.csv'),index=False)
position_data_df.to_csv(os.path.join('csv','SDIC_pos_summary_t1_f__' + date +'.csv'),index=False)
balance_data_df.to_csv(os.path.join('csv','SDIC_balances_summary_t1_f__' + date +'.csv'),index=False)
print("!!!成功!!!")
input("按回车以继续")


