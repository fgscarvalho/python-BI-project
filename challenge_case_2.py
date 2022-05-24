import csv
import mysql.connector
from datetime import date

#generate the cursors
def create_cursor(queries):
    curs = con.cursor()
    curs.execute(queries)
    lines = curs.fetchall()
    curs.close()
    return lines

def initial_index(list):
    cont =0

    for l in list:  
        if l["date:"] == date(2019, 10, 1):
            return cont
        cont += 1

def key_store_code(e):
    return e['store_code:']

def key_loja(e):
    return e['Loja:']

def sum_val(list):
    sum=0
    for value in list:
        sum += value
         
    return sum

def mean_(list):
    mean = []
    for value in list:
        mean.append(sum_val(value)/len(value))
    return mean
    
def avg_list(list,column):
    avg_list = []
    idx = 0
    idx_store = 1
    avg_value = []
    for line in list:   
        if line['store_code:'] == idx_store:
            avg_value.append(line[column])
            idx += 1
        else:
            idx_store += 1
            avg_list.append(avg_value)
            avg_value = []
            avg_value.append(line[column])
    avg_list.append(avg_value)
    return avg_list

def tm(list_1,list_2):
    tm_list = []
    for x in range(0,len(list_1)):
        tm = list_1[x]/list_2[x]
        tm_list.append(round(tm,2))
    return tm_list

def gen_list_dsc(lines):
    mylist = []
    for line in lines:
        my_dict = {}
        my_dict['store_code:'] = line[0]
        my_dict['store_name:'] = line[1]
        my_dict['start_date:'] = line[2]
        my_dict['end_date:'] = line[3]
        my_dict['business_name:'] = line[4]
        my_dict['business_code:'] = line[5]
    
        mylist.append(my_dict)
    return mylist

def gen_list_dss(lines):
    mylist = []
    for line in lines:
        my_dict = {}
        my_dict['store_code:'] = line[0]
        my_dict['date:'] = line[1]
        my_dict['sales_value:'] = line[2]
        my_dict['sales_qty:'] = line[3]
    
        mylist.append(my_dict)
    return mylist

def gen_final_list(tm_,list):
    final_list = []
    for a in range(0,len(tm_)):
        my_dict = {}
        my_dict['Loja:'] = list[a]['store_name:']
        my_dict['Categoria:'] = list[a]['business_name:']
        my_dict['TM:'] = tm_[a]
    
        final_list.append(my_dict)
    return final_list

def main():
    #first query
    query_1 = ("SELECT STORE_CODE, STORE_NAME, START_DATE, END_DATE,BUSINESS_NAME, BUSINESS_CODE FROM data_store_cad")
    lines = create_cursor(query_1)   
    mylist = gen_list_dsc(lines)

    #second query
    query_2 = "SELECT STORE_CODE, DATE, SALES_VALUE, SALES_QTY FROM data_store_sales WHERE DATE BETWEEN '2019-01-01' AND '2019-12-31';"
    lines_2 = create_cursor(query_2)
    mylist_2 = gen_list_dss(lines_2)
    


    #filter date in ['2019-10-01','2019-12-31']
    new_list = []
    length_list = len(mylist_2)
    init_idx = initial_index(mylist_2)

    for x in range(init_idx,length_list):
        new_list.append(mylist_2[x])

    new_list.sort(key=key_store_code)


    list_value = avg_list(new_list,'sales_value:')
    list_qty = avg_list(new_list,'sales_qty:')
    mean_value = mean_(list_value)  
    mean_qty = mean_(list_qty)

    tm_ = tm(mean_value,mean_qty)

    final_list = gen_final_list(tm_,mylist)

    final_list.sort(key=key_loja)

    return final_list
#entrar no servidor Mysql: looqbox-challenge
con = mysql.connector.connect(host='host',database='DB',user='User',password='Password')

if con.is_connected():
    
    data_frame = main()

keys = data_frame[0].keys()

with open('data_frame.csv', 'w', newline='', encoding='utf-8-sig') as csvfile:
    fieldnames = keys
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(data_frame)
    
csvfile.close()

#close all
if con.is_connected():
    con.close()

    print("MySQL disconnected!")