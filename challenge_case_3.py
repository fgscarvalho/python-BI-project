import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np
import mysql.connector


con = mysql.connector.connect(host='host',database='DB',user='User',password='Password')
#generate the cursors
def create_cursor(queries):
    curs = con.cursor()
    curs.execute(queries)
    lines = curs.fetchall()
    curs.close()
    return lines

#utilize dataframe
def use_df(_df,opt):
    first_list=[]
    second_list=[]
    for line in _df:
        first_list.append(line[0])
        second_list.append(line[1])
    try:
        if opt == 1:
            return first_list
        elif opt == 2:
            return second_list
    except:
        print('Wrong option, please choose 1 or 2!')
        
def use_df_per_ten(_df):
    one_list=[]
    for line in _df:
        value_per_ten = line[0]/10
        one_list.append(value_per_ten)
    return one_list

def main():
    query_1 = 'SELECT AVG(rating), year FROM IMDB_movies GROUP BY year;'
    query_2 = 'SELECT AVG(metascore), year FROM IMDB_movies GROUP BY year;'
    
    
    rating_df = create_cursor(query_1)
    
    rating_list = use_df(rating_df,1)
    year_list = use_df(rating_df,2)
    
    
    meta_score_df = create_cursor(query_2)
    
    meta_list= use_df_per_ten(meta_score_df)
    


    fig, ax = plt.subplots(figsize=(5, 2.7), layout='constrained')
    ax.plot(year_list,rating_list, label='IMDB Rating')
    ax.plot(year_list,meta_list, label='Metascore per 10')
    ax.set_xlabel('Years')  # Add an x-label to the axes.
    ax.set_ylabel('Value')  # Add a y-label to the axes.
    ax.set_title("Rating comparations")  # Add a title to the axes.
    ax.legend();  # Add a legend.
    plt.show()

    
    query_3 = 'SELECT SUM(RevenueMillions), rating FROM IMDB_movies GROUP BY rating;'
    
    revenue_millions_df = create_cursor(query_3)
    
    revenue_list= use_df(revenue_millions_df,1)
    rating_scale= use_df(revenue_millions_df,2)


    print(len(revenue_list))
    
    fig, ax = plt.subplots(figsize=(5, 2.7), layout='constrained')
    ax.bar(rating_scale,revenue_list)
    ax.set_xlabel('IMDB Rating')  # Add an x-label to the axes.
    ax.set_ylabel('Revenue in Millions')  # Add a y-label to the axes.
    ax.set_title("Revenue and rating comparations")  # Add a title to the axes.
    ax.legend();  # Add a legend.
    plt.show() #Show the graph.

if con.is_connected():
    #create cursor
    main()

if con.is_connected():
   con.close()
   print("MySQL disconnected!")