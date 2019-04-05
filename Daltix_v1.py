# -*- coding: utf-8 -*-
"""
Created on Tue Apr  2 18:57:18 2019

@author: anton
"""

#load libraries and imports, from json to dataframe

#import os

#import string


import json

import pandas as pd

# fuzz is used to compare TWO strings
from fuzzywuzzy import fuzz

# process is used to compare a string to MULTIPLE other strings
from fuzzywuzzy import process



# Set ipython's max row display
pd.set_option('display.max_row', 1000)

# Set iPython's max column width to 50
pd.set_option('display.max_columns', 50)


data = [json.loads(line) for line in open('D:/Daltix/DataScienceTest-master/data/dataset.jsonl', 'r')]

df = pd.DataFrame.from_dict(data, orient='columns')

#Rename columns to make easier to refer
df.columns = ['brand', 'contents', 'id', 'description','url', 'name', 'product_id','shop', 'specifications']


#for i in range(len(df)):
for i in range(len(df)):
    possibilities = process.extract(df.iloc[i,5], df['name'], limit=2, scorer=fuzz.token_sort_ratio)
    poss0 = pd.DataFrame(possibilities[0])
    poss0 = poss0.transpose()
    poss0.columns = ['record0', 'record1', 'record2']
    poss1 = pd.DataFrame(possibilities[1])
    poss1 = poss1.transpose()
    poss1.columns = ['record3', 'record4', 'record5']
    poss2 = pd.concat([poss0,poss1], axis = 1)
    if i == 0:
        poss = pd.DataFrame(poss2)
    else:
        poss = poss.append(poss2)
        poss.to_csv('D:/Daltix/DataScienceTest-master/data/poss.csv', index = False )
        print(i)




for i in range(len(df)):
    zero = pd.DataFrame(poss.iloc[i,[2]].tolist())
    zero.columns = ['idx0']
    one = pd.DataFrame(poss.iloc[i,[5]].tolist())
    one.columns = ['idx1']
    three = pd.concat([zero,one], axis = 1)
    if i == 0:
        four = pd.DataFrame(three)
    else:
        four = four.append(three)
        
  
        

for j in range(len(df)):
    zero = pd.DataFrame(four.iloc[j,[0]].tolist())
    zero.columns = ['idx0']
    zero_a = int(zero.at[0,'idx0'])
    daltix_id_one = pd.DataFrame(df.iloc[zero_a,[2]])
    daltix_id_one.columns = ['daltix_id_1']
    one = pd.DataFrame(four.iloc[j,[1]].tolist())
    one.columns = ['idx1']
    one_a = int(one.at[0,'idx1'])
    daltix_id_two = pd.DataFrame(df.iloc[one_a,[2]])
    daltix_id_two.columns = ['daltix_id_2']
    subm_aux = pd.concat([daltix_id_one, daltix_id_two], axis = 1)
    if j == 0:
        subm = pd.DataFrame(subm_aux)
    else:
        subm = subm.append(subm_aux)

subm.drop(subm.columns[0], axis = 1)         



subm.to_csv('/home/antonio/Documentos/Daltix/DataScienceTest-master/data/dataset.jsonl/submission.csv', index = False )



   