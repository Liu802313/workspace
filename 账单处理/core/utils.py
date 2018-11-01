#!/usr/bin/env python
import chardet
class Tools():
    def ByteDecode(self,data):
        encodeMode = chardet.detect(data[:100])['encoding']
        data = data.decode(encodeMode)
        return data
    
    def translate(self,list_trans,trans):
        translation = list(map(lambda x:trans[x] if x in trans.keys() else x,list_trans))
        return translation
