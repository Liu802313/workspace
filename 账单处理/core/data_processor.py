#!/usr/bin/env python
import numpy as np 
import pandas as pd 
import os
import chardet
import re
from core.utils import Tools

class Dataloader():
    def __init__(self,column_trade,column_position,column_balance):        
        self.column_trade = column_trade
        self.column_position = column_position
        self.column_balance = column_balance
        self.all_df = {}

    def split_line(self,line):
        #分割单行中含有列名和数字的数据
        data = re.split(r':\s*|：+\s*|\s+',line)
        data = [i for i in data if not bool(re.search(r'[a-zA-Z]',i))]

        return data

    def get_df_ogz(self,line,filelist):
        '''
        获取机构版某个表的DataFrame
        '''
        l = line
        # df_ogz = {}
        if (('---' in filelist[line+3]) and ('---' in filelist[line+1])):
            col = filelist[line+2].split()
            l = line + 4
            datalist = []
            datalist.append(filelist[l].split())
            for i in range(l + 1,len(filelist)):
                if len(filelist[i].split()) != len(filelist[l].split()):
                    break
                datalist.append(filelist[i].split())
            df = pd.DataFrame(data=datalist,columns=col)
        else:
            #期货客户帐单_资金清单 要特殊处理
            if filelist[line] != '期货客户帐单_资金清单':
                return
            l = line + 2
            datalist = []
            for i in range(l,len(filelist)):                       
                if '---' in filelist[i]:
                    break
                datalist.append(filelist[i].split())
            df = pd.DataFrame(data=datalist)
            data = []
            data.extend(df[1].tolist())
            data.extend(df[3].tolist())
            data = [[x for x in data if x is not None]]
            col = []
            col.extend(df[0].tolist())
            col.extend(df[2].tolist())     
            col = [x[:-1] for x in col if x is not None]
            df = pd.DataFrame(data=data,columns=col)
        return df

    def get_df_ctp(self,line,filelist):
        '''
        获取ctp账单某个表的DataFrame
        '''
        l = line
        if (('---' in filelist[line + 1]) and ('---' in filelist[line+4])):
            col = filelist[line+2].split()
            l = line + 5
            datalist = []
            datalist.append(filelist[l].split())
            for i in range(l + 1,len(filelist)):
                if len(filelist[i].split()) != len(filelist[l].split()):
                    break
                datalist.append(filelist[i].split())
            df = pd.DataFrame(data=datalist,columns=col)
        else:
            def get_table(line):
                for t in ['交易结算单(逐笔)','资金状况']:
                    if t in line:
                        return t
            t = get_table(filelist[line])
            if (t not in ['交易结算单(逐笔)','资金状况']):
                return
            if(t == '资金状况'):
                l = line + 2
            else:
                l = line + 1
            datalist = []
            for i in range(l,len(filelist)):                       
                if len(filelist[i]) is 0:
                    break
                datalist.append(self.split_line(filelist[i]))
            df = pd.DataFrame(data=datalist)
            data = []
            data.extend(df[1].tolist())
            data.extend(df[3].tolist())
            data = [[x for x in data if x is not None]]
            col = []
            col.extend(df[0].tolist())
            col.extend(df[2].tolist())     
            col = [x for x in col if x is not None]
            df = pd.DataFrame(data=data,columns=col)
        return df

    def get_all_df(self,organize_table,ctp_table,organize_path,ctp_path,replace):
        '''
        获得所有txt中需要的表格数据
        return key为各个表名,value为各表对应的DataFrame的dict
        '''
        filename_ogz = []
        tool = Tools()
        all_df = []
        for i in os.listdir(organize_path):
            if os.path.splitext(i)[1] ==".txt":
                filename_ogz.append(i)
        for file_ in filename_ogz:
            filelist = []
            with open (os.path.join(organize_path, file_),'rb') as f:
                data = f.read()
                data = tool.ByteDecode(data)
                filelist = data.split('\n')
            #删除第一个·\
            for i in range(len(filelist)):
                if len(filelist[i]) == 0 :
                    continue
                if(filelist[i][0] == '.'):
                    filelist[i] = filelist[i][1:]
                    filelist[i] = filelist[i].strip()
                for key,value in replace.items():
                    if key in filelist[i]:
                        filelist[i] = filelist[i].replace(key,value)
            df = {}
            for i in range(len(filelist)):                
                key = [x for x in organize_table if x in filelist[i]]
                if(len(key)>0):
                    df[key[0]] = self.get_df_ogz(i,filelist)
            #特殊处理，把客户号整合进各表格中
            for key in df.keys():
                if key != '交易结算单':
                    df[key]['资产帐号'] = df['交易结算单']['资产帐号'][0]
                    df[key]['结算日'] = df['交易结算单']['结束时间'][0]
            if df != {}:
                all_df.append(df)

        filename_ctp = []
        for i in os.listdir(ctp_path):
            if os.path.splitext(i)[1] ==".txt":
                filename_ctp.append(i)
        for file_ in filename_ctp:
            filelist = []
            with open (os.path.join(ctp_path, file_),'rb') as f:
                data = f.read()
                data = tool.ByteDecode(data)
                filelist = data.split('\n')

            for i in range(len(filelist)):
                filelist[i] = filelist[i].replace('|',' ')
                filelist[i] = filelist[i].strip()
                for key,value in replace.items():
                    if key in filelist[i]:
                        filelist[i] = filelist[i].replace(key,value)
            df = {}
            for i in range(len(filelist)):
                key = [x for x in ctp_table if x in filelist[i]]
                if(len(key)>0):
                    df[key[0]] = self.get_df_ctp(i,filelist)

            #特殊处理，把客户号整合进各表格中
            for key in df.keys():
                if key != '交易结算单(逐笔)':
                    df[key]['客户号'] = df['交易结算单(逐笔)']['客户号'][0]
                    df[key]['结算日'] = df['交易结算单(逐笔)']['日期'][0]
            if df != {}:
                all_df.append(df)
        
        #最后将数组合并，整合成一个dict
        for df in all_df:
            for key,value in df.items():
                if key in self.all_df.keys():
                    self.all_df[key] = pd.concat([self.all_df[key],value])
                else:
                    self.all_df[key] = value
        
        for key,value in self.all_df.items():
            self.all_df[key] = value.reset_index()


    def get_common_data(self,config,trans):
        '''
        通用数据的处理
        return trade_data_df,position_data_df,balance_data_df三个DataFrame
        '''
        trade_data_df = pd.DataFrame([],columns=self.column_trade)
        position_data_df = pd.DataFrame([],columns=self.column_position)
        balance_data_df = pd.DataFrame([],columns=self.column_balance)
        
        def get_df(config,data_df):
        #处理不同取值规则的列   
            tool = Tools()
            for col,values in config.items():
                m = []
                if type(values).__name__  == 'list':
                    for v in values:
                        if v['table'] not in self.all_df.keys():
                            continue
                        try:
                            if(v['type'] == 'common'):
                                m.extend(list(self.all_df[v['table']][v['column']]))
                            elif(v['type'] == 'translate'):
                                m.extend(tool.translate(list(self.all_df[v['table']][v['column']]),trans))
                            elif(v['type'] == 'add'):
                                df1 = pd.to_numeric(self.all_df[v['table']][v['column'][0]], errors='ignore').fillna(0)
                                df2 = pd.to_numeric(self.all_df[v['table']][v['column'][1]], errors='ignore').fillna(0)
                                m.extend( list(df1 + df2))
                            elif(v['type'] == 'sub'):
                                df1 = pd.to_numeric(self.all_df[v['table']][v['column'][0]], errors='ignore').fillna(0)
                                df2 = pd.to_numeric(self.all_df[v['table']][v['column'][1]], errors='ignore').fillna(0)
                                m.extend(list(df1 - df2))
                            else:
                                m = ''
                        except:
                            m = ''
                    data_df[col] = m
                else:
                    data_df[col] = values
            return data_df
        
        #在这里生成trade
        trade_data_df = get_df(config['trade'],trade_data_df)
        #在这里生成position
        position_data_df = get_df(config['position'],position_data_df)
        #在这里生成balance
        balance_data_df = get_df(config['balance'],balance_data_df)
        
        return trade_data_df,position_data_df,balance_data_df

    def get_trade_data(self,trade_data_df,contract,trans):
        #在这里定制trade的特殊列
        tool = Tools()
        trade_data_df['Buy'] = list(map(lambda x: x if x == 'B' else '',list(trade_data_df['Buy'])))
        trade_data_df['Sell'] = list(map(lambda x: x if x == 'S' else '',list(trade_data_df['Sell'])))
        try:
            contract_code = list(trade_data_df['Contract Code'])
            month,year,con = [],[],[]
            for code in contract_code:
                is_exist = False
                for k in contract.keys():
                    if k in code:
                        is_exist = True
                        con.append(k)
                        date = code.replace(k,'')
                        month.append(date[-2:])
                        year.append('1' + date[:-2] if len(date[:-2]) == 1 else date[:-2])
                        break
                if not is_exist:
                    con.append('')
                    month.append('')
                    year.append('')
            trade_data_df['Contract Code'] = con
            trade_data_df['Month'] = month
            trade_data_df['Year'] = year
            trade_data_df['Exchange Code'] = tool.translate(trade_data_df['Exchange Name'],trans)
            description = [tool.translate(con,trans)[i] + ' ' + year[i] + ' ' + tool.translate(month,trans)[i] for i in range(len(con))]
            trade_data_df['Contract Description'] = description
        except:
            pass
        return trade_data_df

    def get_position_data(self,position_data_df,contract,trans,trade_date_dict):
        #在这里定制position的特殊列
        tool = Tools()
        try:
            contract_code = list(position_data_df['Contract Code'])
            month,year,con,exchange = [],[],[],[]
            for code in contract_code:
                is_exist = False
                for k,v in contract.items():
                    if k in code:
                        is_exist = True
                        con.append(k)
                        date = code.replace(k,'')
                        month.append(date[-2:])
                        year.append('1' + date[:-2] if len(date[:-2]) == 1 else date[:-2])
                        exchange.append(v)
                        break
                if not is_exist:
                    con.append('')
                    month.append('')
                    year.append('')
                    exchange.append('')    

            position_data_df['Contract Code'] = con
            position_data_df['Month'] = month
            position_data_df['Year'] = year
            position_data_df['Exchange Name'] = exchange
            position_data_df['Exchange Code'] = tool.translate(exchange,trans)

            date = [year[i] + month[i] for i in range(len(con))]
            position_data_df['Last Trading Date'] = list(map(lambda x: trade_date_dict[int(x)] if int(x) in trade_date_dict.keys() else '',date))
            description = [tool.translate(con,trans)[i] + ' ' + year[i] + ' ' + tool.translate(month,trans)[i] for i in range(len(con))]
            position_data_df['Contract Description'] = description     

        except:
            pass
        return position_data_df

    def get_balance_data(self,balance_data_df):
        #在这里定制balance的特殊列
        return balance_data_df