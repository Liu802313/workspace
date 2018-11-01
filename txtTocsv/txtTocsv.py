#-*-coding:utf-8 -*-
import csv
import os
import re
import json
import chardet

def ByteDecode(data):
    encodeMode = chardet.detect(data[:100])['encoding']
    data = data.decode(encodeMode)
    return data

realList = os.listdir('机构版')
fileList = []
for filepath in realList:
    #if(os.path.isfile(filepath)):
    if(os.path.splitext(filepath)[1] ==".txt"):
        fileList.append(filepath)
os.chdir('机构版')

for fileName in fileList:
    print(fileName)
    dictTrans = {}
    with open ("dictionary.json", 'rb') as f:
        data = f.read()
        data = ByteDecode(data)
        dictTrans = json.loads(data)
    print(dictTrans)

    dictNode = {}
    with open ("totalSetting.json", 'rb') as f:
        data = f.read()
        data = ByteDecode(data)
        dictNode = json.loads(data) #记录标题行数
    print(dictNode)
    
    with open(fileName,'rb')  as f:
        data = f.read()
        data = ByteDecode(data)
        lineList = data.split('\n')
        
    TempList = [] #记录分隔符'------'行数
    existPoint = True #是否含有.作为行的开头
    #获取最终需要的行数
    def getLine(tempLineNum):
        for i in range(tempLineNum-2,-1,-1):
            if  lineList[i] in dictNode.keys():
                continue
            if len(lineList[i]) != 0:
                return i
        return -1

    for i in range(len(lineList)):    
        if len(lineList[i]) == 0:
            continue
        #删除每一行的'.'
        if lineList[i][0] == '.':
            lineList[i] = lineList[i][1:]
        lineList[i] = '|'.join(lineList[i].strip().split()) #将单行内容转化为用|连接的一行
        for key in dictNode.keys():
            if key in lineList[i]:
                dictNode[key]['row'] = i
        #记录分隔符'--------'的行数
        if '--------' in lineList[i]:
            TempList.append(i)
    print(dictNode)
    print(TempList)
    #先将标题行数和分隔符'-----'行数放一起并排序，方便后续拿取需要的行数
    for value in dictNode.values():
        if value['row'] != 0:
            TempList.append(value['row']) 
    TempList.sort()
    print(TempList)
    for key,value in dictNode.items():
        if value['row'] == 0:
            break
        print(key)
        tempLineNum = 0
        if key == '期货客户帐单_资金流水单':
            tempLineNum = TempList[TempList.index(value['row'] ) + 2]
        else:
            tempLineNum = TempList[TempList.index(value['row'] ) + 3]
        i = getLine(tempLineNum)
        lstr = lineList[i].split('|')#合计行内容
        lline = list(map(int,value['colume'].split(',')))#行数
        totalline = [''] *max(lline)
        dicTotalline = dict(zip(lline, lstr))
        for key,value in dicTotalline.items():
            totalline[key-1] = value
        print(totalline)
        lineList[i] = '|'.join(totalline)

    with open(os.path.splitext(fileName)[0] + '_zh_CN.csv', 'w',encoding = 'gbk',newline='',errors = 'ignore') as csvfile:
        spamwriter = csv.writer(csvfile, dialect='excel')
        for line in lineList:
            line_list = line.split('|')
            spamwriter.writerow(line_list)

    with open(os.path.splitext(fileName)[0] + '_en_US.csv', 'w',encoding = 'gbk',newline='',errors = 'ignore') as csvfile:
        spamwriter = csv.writer(csvfile, dialect='excel')
        for line in lineList:
            line_list = line.split('|')
            for word in line_list:
                if word in dictTrans.keys():
                    line_list[line_list.index(word)] = dictTrans[word]
            spamwriter.writerow(line_list)
os.chdir('..')
realList = os.listdir('CTP')
fileList = []
for filepath in realList:
    #if(os.path.isfile(filepath)):
    if(os.path.splitext(filepath)[1] ==".txt"):
        fileList.append(filepath)
os.chdir('CTP')

for fileName in fileList:
    print(fileName)
    dictTrans = {}
    with open ("dictionary.json", 'rb') as f:
        data = f.read()
        data = ByteDecode(data)
        dictTrans = json.loads(data)
    print(dictTrans)

    with open(fileName,'rb')  as f:
        data = f.read()
        encodeMode = chardet.detect(data[:100])['encoding']
        data = data.decode(encodeMode)
        lineList = data.split('\n')

    def repalceStr(line):
        for  eachKey,eachValue in dictTrans.items():
            if line == eachKey:
                line = eachKey + '/' + eachValue
        return line    

    for i in range(len(lineList)):    
        if len(lineList[i]) == 0:
            continue
        lineList[i] = ' '.join(lineList[i].strip().split()) #去除首尾的空格并将多余空格删除只保留一个


    with open(os.path.splitext(fileName)[0] + '.csv', 'w',encoding = 'gbk',newline='',errors = 'ignore') as csvfile:
        spamwriter = csv.writer(csvfile, dialect='excel')
        for line in lineList:            
            line_list = []
            if '|' in line:
                line = repalceStr(line)#增加翻译
                if line[0] == '|':
                    line = line[1:]
                line_list = (''.join(line.strip().split())).split('|')
                for line in line_list:
                    line_list[line_list.index(line)] = repalceStr(line)
            elif ':' in line:
                line_list = re.split(r':\s*|[0-9]\s+',line)
            else:                
                line_list.append(line)

            spamwriter.writerow(line_list)

input('处理完毕，请按回车键继续...')
