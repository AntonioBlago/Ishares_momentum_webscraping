# -*- coding: utf-8 -*-
"""
Created on Fri Dec 13 13:34:11 2019

@author: antonio.blago
"""

from bs4 import BeautifulSoup
import urllib.request
import requests
import pandas as pd
import csv
import time

search_m="https://www.ishares.com/de/privatanleger/de/produkte/270051/ishares-msci-world-momentum-factor-ucits-etf/1478358465952.ajax?fileType=csv&fileName=IS3R_holdings&dataType=fund"

list_funds=[]



with requests.Session() as s:
    download = s.get(search_m)

    decoded_content = download.content.decode('utf-8')
    
    cr = csv.reader(decoded_content.splitlines(), delimiter=',')
    my_list = list(cr)
    list_new=[]
    for row in my_list:
        if len(row)>3:
            list_new.append(row)

Ishares_momentum=pd.DataFrame(list_new)

Ishares_momentum.rename(columns=Ishares_momentum.iloc[0],inplace=True)

Ishares_momentum.drop(0,inplace=True)



#%%

import numpy as np

additional_data=[]
count=-1


for index, row in Ishares_momentum.iterrows():
    count=count+1
    
    if len(row["ISIN"])>5: #and  np.isnan(row["1 Monat"])==True:
    
        print("Query: "+ str(count) + "/" + str(Ishares_momentum.shape[0]))
       
        time.sleep(2)
        
        try:
            base="https://www.comdirect.de/inf/aktien/{}".format(str(row["ISIN"]))
            ISIN=str(row["ISIN"])
            response = urllib.request.urlopen(base)
            html = response.read()
            # Parsing response
            soup = BeautifulSoup(html, 'html.parser')
            id_kennzahlen=soup.find("a",{"class":"button button--secondary button--medium"})
            #button button--secondary button--medium
            id_kennzahlen=str(id_kennzahlen)
            
            pos=id_kennzahlen.find("NOTATION")
            
            
            Notation_id=id_kennzahlen[pos+9:pos+10+8]
            Notation_id=Notation_id.replace("'","")
            Notation_id=Notation_id.replace("}","")
            Notation_id=Notation_id.replace('"','')
            
            kennzahlen_html="https://www.comdirect.de/inf/aktien/detail/kennzahlen.html?ID_NOTATION={}".format(Notation_id)
            
            response2= urllib.request.urlopen(kennzahlen_html)
            html2 = response2.read()
            # Parsing response
            soup2= BeautifulSoup(html2, 'html.parser')
                        
        
                
            mydivs = soup2.find_all("div", {"class": "table__container--scroll"})
            
            row_data = []
            Performance=[]
            Volatilität=[]
            Beta=[]
            
            for elem in mydivs:
                
                for table in elem.find_all("table"):
                    
                    for row in table.find_all("tr"):
                        cols = row.find_all("td")
                        cols = [ele.text.strip() for ele in cols]
                        row_data.append(cols)
                        
            
            Performance=row_data[1][3:]
            Performance=[x.replace("+","") for x in Performance]
            Performance=[x.replace("\xa0%","") for x in Performance]
            Performance=[float(x.replace(",",".")) for x in Performance]
            
            Volatilität=row_data[4][3:]
            Volatilität=[x.replace("+","") for x in  Volatilität]
            Volatilität=[x.replace("\xa0%","") for x in  Volatilität]
            Volatilität=[float(x.replace(",",".")) for x in  Volatilität]
            
            Beta=row_data[11][3:]
            Beta=[x.replace("+","") for x in  Beta]
            Beta=[x.replace("\xa0%","") for x in  Beta]
            Beta=[float(x.replace(",",".")) for x in  Beta]
            
            for x1 in Volatilität:
                Performance.append(x1)
            
            for x1 in Beta:
                Performance.append(x1)
            
            
            
            Performance.append(ISIN)
            
            additional_data.append(Performance)
            

            
            print('Comdirect Job done: '+ ISIN)
        
            
        except:
            print('Error in comdirect: '+ ISIN)
            #break

        
#%%
performance=pd.DataFrame(additional_data,columns=["Perf. 1 Monat","Perf. 3 Monate","Perf. 1 Jahr","Perf. 3 Jahre","Perf. 5 Jahre",
                                                  "Vola. 1 Monat","Vola. 3 Monate","Vola. 1 Jahr","Vola. 3 Jahre","Vola. 5 Jahre",
                                                  "Beta 1 Monat","Beta 3 Monate","Beta 1 Jahr","Beta 3 Jahre","Beta 5 Jahre",
                                                  "ISIN"])


Ishares_momentum_new=pd.merge(Ishares_momentum,performance,how="outer",on="ISIN")

Ishares_momentum_new["Sharpe Ratio 1 Monat"]=Ishares_momentum_new["Perf. 1 Monat"]/Ishares_momentum_new["Vola. 1 Monat"]
Ishares_momentum_new["Sharpe Ratio 3 Monate"]=Ishares_momentum_new["Perf. 3 Monate"]/Ishares_momentum_new["Vola. 3 Monate"]
Ishares_momentum_new["Sharpe Ratio 1 Jahr"]=Ishares_momentum_new["Perf. 1 Jahr"]/Ishares_momentum_new["Vola. 1 Jahr"]
Ishares_momentum_new["Sharpe Ratio 3 Jahre"]=Ishares_momentum_new["Perf. 3 Jahre"]/Ishares_momentum_new["Vola. 3 Jahre"]
Ishares_momentum_new["Sharpe Ratio 5 Jahre"]=Ishares_momentum_new["Perf. 5 Jahre"]/Ishares_momentum_new["Vola. 5 Jahre"]



#%%
#import numpy as np

additional_data2=[]
count=-1


for index, row in Ishares_momentum.iterrows():
    count=count+1
    
    if len(row["ISIN"])>5: #and  np.isnan(row["1 Monat"])==True:
    
        print("Query: "+ str(count) + "/" + str(Ishares_momentum.shape[0]))
       
        time.sleep(2)
        
        try:
            base="https://www.comdirect.de/inf/aktien/{}".format(str(row["ISIN"]))
            ISIN=str(row["ISIN"])
            response = urllib.request.urlopen(base)
            html2 = response.read()
            soup2= BeautifulSoup(html2, 'html.parser')
            mydivs = soup2.find_all("tr", {"class": "simple-table__row"})
            
            row_data = []
            
            
            for xx in range(2,5):#len(mydivs)
                #print(mydivs[xx].text.replace("\n",""))
                
                if "KGVe" in mydivs[xx].text.replace("\n",""):
                    KGV=mydivs[xx].text.replace("\n","")
                    KGV= KGV.replace("KGVe","")
                    KGV= KGV.replace(" ","")
                    KGV= KGV.replace(",",".")
                    row_data.append(float(KGV))
                    #break
                if "DIVe" in mydivs[xx].text.replace("\n",""):
                    DIV=mydivs[xx].text.replace("\n","")
                    DIV=DIV.replace("DIVe","")
                    DIV=DIV.replace("%","")
                    DIV= DIV.replace(" ","")
                    DIV= DIV.replace(",",".")
                    row_data.append(float(DIV))
                    row_data.append(ISIN)
                    #break
            
            
            additional_data2.append(row_data)
                    
            print('Comdirect Job done: '+ ISIN)
            #break
        
            
        except:
            print('Error in comdirect: '+ ISIN)
            #break

            
                        
                                             
            
         
            
#%%
            
performance2=pd.DataFrame(additional_data2,columns=["KGVe","DIVe","ISIN"])
Ishares_momentum_new2=pd.merge(Ishares_momentum_new,performance2,how="outer",on="ISIN")

Ishares_momentum_new2["KGVe"]=Ishares_momentum_new2["KGVe"].replace("--",0)
Ishares_momentum_new2["DIVe"]=Ishares_momentum_new2["DIVe"].replace("--",0)


            
#%%
            


import datetime as dt

date=dt.date.today()

#name="Ishares_momentum"+str(date)

Ishares_momentum_new2.to_excel("Ishares_momentum-{}.xlsx".format(date))
    
