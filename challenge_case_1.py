import csv
import mysql.connector
from datetime import datetime

#generate the cursors
def create_cursor(queries):
    curs = con.cursor()
    curs.execute(queries)
    lines = curs.fetchall()
    curs.close()
    return lines

#verifie date_iso
def datetime_valid(dt_str):
    try:
        datetime.fromisoformat(dt_str)
    except:
        return False
    return True

#Verifie integer
def int_valid(val):
    if int(val)==val:
        return True
    else:
        return False

#Verifie float
def float_valid(value):
    if float(value)==value:
        return True
    else:
        return False

def gen_list(lines):
    
    #Save the data in a list of dictionaries
    list_ = []
    for line in lines:
        
        my_dict = {}
        my_dict['store_code:'] = line[0]
        my_dict['product_code:'] = line[1]
        my_dict['date:'] = line[2]
        my_dict['sales_value:'] = line[3]
        my_dict['sales_qty:'] = line[4]
    
        list_.append(my_dict)
    return list_

#define function retrieve
def retrieve_data(**parametros):
    query =''
    for idx, (key, value) in enumerate(parametros.items()):
        if key == 'date' and datetime_valid(value) == False and datetime.fromisoformat(value).year != 2019 :
            TypeError("Date Syntax Error!")
            break
        elif (key == 'product_code' or key == 'store_code' or key == 'sales_qty') and int_valid(value) == False:
            TypeError("The product_code, store_code or sales_qty is not integer!")
            break
        elif key == 'sales_value' and float_valid(value) == False:
            TypeError("The sales_value is not float!")
        elif key == 'date' and datetime_valid(value) == True and datetime.fromisoformat(value).year == 2019 :
            value = "'" + value + "'"

        
        value = str(value)
        if idx == 0:
            query = "SELECT * FROM data_product_sales WHERE " + key +" = " + value
        else:
            query += " AND " + key + " = " + value
    
    
    
    print("Your query is:\n" + query)

    
    lines = create_cursor(query)

    mylist = gen_list(lines)
    
    return mylist

#call funtion
#You can call all parameters from table data_product_sales.
#Ex: retrieve_data(product_code = 18, store_code = 1, date = "'2019-02-04'", sales_value = 708.50, sales_qty = 65)
def main():
    my_data = retrieve_data(product_code = 18 ,store_code = 1, date = '2019-12-30')

    if my_data == []:
        print("Dataframe not found!")
    if con.is_connected():
        con.close()
        print("MySQL disconnected!")
    
    return my_data

#Connect  to MySQL server:
con = mysql.connector.connect(host='host',database='DB',user='User',password='password')
if con.is_connected():
    my_data = main()

keys = my_data[0].keys()

#export in .csv 
#Before run, change the file name
with open('my_data.csv', 'w', newline='', encoding='utf-8-sig') as csvfile:
    fieldnames = keys
    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

    writer.writeheader()
    writer.writerows(my_data)
    
csvfile.close()

#close all
if con.is_connected():
    con.close()
    print("MySQL disconnected!")
