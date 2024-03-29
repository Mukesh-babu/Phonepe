import os
import json
import pandas as pd
import mysql.connector
import plotly.express as px
from sqlalchemy import create_engine
import streamlit as st
from streamlit_option_menu import option_menu
import requests
import PIL
from PIL import Image
import numpy as np

def clean_state_name(state):
    return state.replace('andaman-&-nicobar-islands', 'Andaman & Nicobar') \
                .replace('-', ' ') \
                .title() \
                .replace('Dadra & Nagar Haveli & Daman & Diu', 'Dadra and Nagar Haveli and Daman and Diu')

def process_data(path, columns, data_key, sub_key, count_key, amount_key):
    data_list = os.listdir(path)

    for state in data_list:
        cur_states = os.path.join(path, state)
        year_list = os.listdir(cur_states)

        for year in year_list:
            cur_year = os.path.join(cur_states, year)
            file_list = os.listdir(cur_year)

            for file in file_list:
                cur_file = os.path.join(cur_year, file)
                with open(cur_file, 'r') as data_file:
                    data = json.load(data_file)

                try:
                    for i in data['data'][data_key]:
                        entity_name = i[sub_key]
                        count = i[count_key]
                        amount = i[amount_key]
                        columns[sub_key].append(entity_name)
                        columns['Transaction_count'].append(count)
                        columns['Transaction_amount'].append(amount)
                        columns['States'].append(state)
                        columns['Years'].append(year)
                        columns['Quarter'].append(int(file.strip('.json')))
                except:
                    pass

    df = pd.DataFrame(columns)
    df['States'] = df['States'].apply(clean_state_name)
    df['Transaction_type'] = data_key.capitalize()

    return df

# Process aggregated transaction data
path1 = "C:/Users/FCI/OneDrive/Desktop/New folder/Phonepe/pulse/data/aggregated/transaction/country/india/state/"
columns1 = {'States': [], 'Years': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [], 'Transaction_amount': []}
df_transaction = process_data(path1, columns1, 'transactionData', 'name', 'count', 'amount')

# Process aggregated user data
path2 = "C:/Users/FCI/OneDrive/Desktop/New folder/Phonepe/pulse/data/aggregated/user/country/india/state/"
columns2 = {'States': [], 'Years': [], 'Quarter': [], 'Brands': [], 'Transaction_count': [], 'Percentage': []}
df_user = process_data(path2, columns2, 'usersByDevice', 'brand', 'count', 'percentage')

# Process aggregated insurance data
path3 = "C:/Users/FCI/OneDrive/Desktop/New folder/Phonepe/pulse/data/aggregated/insurance/country/india/state/"
columns3 = {'States': [], 'Years': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [], 'Transaction_amount': []}
df_insurance = process_data(path3, columns3, 'transactionData', 'name', 'count', 'amount')

# Process map transaction data
path4 = 'C:/Users/FCI/OneDrive/Desktop/New folder/Phonepe/pulse/data/map/transaction/hover/country/india/state/'
columns4 = {'States': [], 'Years': [], 'Quarter': [], 'Districts': [], 'Transaction_count': [], 'Transaction_amount': []}
df_map_transaction = process_data(path4, columns4, 'hoverDataList', 'name', 'metric[0].count', 'metric[0].amount')

# Process map user data
path5 = 'C:/Users/FCI/OneDrive/Desktop/New folder/Phonepe/pulse/data/map/user/hover/country/india/state/'
columns5 = {'States': [], 'Years': [], 'Quarter': [], 'Districts': [], 'Registered_Users': [], 'App_Opens': []}
df_map_user = process_data(path5, columns5, 'hoverData', '0', '1.registeredUsers', '1.appOpens')

# Process map insurance data
path6 = "C:/Users/FCI/OneDrive/Desktop/New folder/Phonepe/pulse/data/map/insurance/hover/country/india/state/"
columns6 = {'States': [], 'Years': [], 'Quarter': [], 'Districts': [], 'Transaction_count': [], 'Transaction_amount': []}
df_map_insurance = process_data(path6, columns6, 'hoverDataList', 'name', 'metric[0].count', 'metric[0].amount')

# Process top insurance data
path7 = "C:/Users/FCI/OneDrive/Desktop/New folder/Phonepe/pulse/data/top/insurance/country/india/state/"
columns7 = {'States': [], 'Years': [], 'Quarter': [], 'Pincodes': [], 'Transaction_count': [], 'Transaction_amount': []}
df_top_insurance = process_data(path7, columns7, 'pincodes', 'entityName', 'metric.count', 'metric.amount')

# Process top transaction data
path8 = "C:/Users/FCI/OneDrive/Desktop/New folder/Phonepe/pulse/data/top/transaction/country/india/state/"
columns8 = {'States': [], 'Years': [], 'Quarter': [], 'Pincodes': [], 'Transaction_count': [], 'Transaction_amount': []}
df_top_transaction = process_data(path8, columns8, 'pincodes', 'entityName', 'metric.count', 'metric.amount')

# Process top user data
path9 = "C:/Users/FCI/OneDrive/Desktop/New folder/Phonepe/pulse/data/top/user/country/india/state/"
columns9 = {'States': [], 'Years': [], 'Quarter': [], 'Pincodes': [], 'Registered_Users': []}
df_top_user = process_data(path9, columns9, 'pincodes', 'name', 'registeredUsers', '')

# Clean state names
df_top_user['States'] = df_top_user['States'].apply(clean_state_name)

mycon = mysql.connector.connect(host='localhost', user='root', password='12345',database='phonepe_data')
mycursor = mycon.cursor()

def insert_into_mysql(table_name, df):
    # Assuming you already have a MySQL connection and cursor
    insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['%s' for _ in df.columns])})"
    values = [tuple(row) for row in df.itertuples(index=False, name=None)]

    mycursor.executemany(insert_query, values)
    mycon.commit()

mycon = mysql.connector.connect(host='localhost', user='root', password='12345',database='phonepe_data')
mycursor = mycon.cursor()

#agg_transaction_table
create_query_1 = '''CREATE TABLE IF NOT EXISTS aggregated_transaction (
                        States VARCHAR(200),
                        Years INT, 
                        Quarter INT, 
                        Transaction_type VARCHAR(200),
                        Transaction_count BIGINT, 
                        Transaction_amount BIGINT
                    )'''
mycursor.execute(create_query_1)
mycon.commit()

df_transaction = process_data("C:/Users/FCI/OneDrive/Desktop/New folder/Phonepe/pulse/data/aggregated/transaction/country/india/state/",
                             {'States': [], 'Years': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [], 'Transaction_amount': []},
                             'transactionData', 'name', 'count', 'amount')
insert_into_mysql("aggregated_transaction", df_transaction)

create_query_2= '''Create table if not exists aggregated_user (States varchar(200),
                                                        Years int, 
                                                        Quarter int, 
                                                        Brands varchar(200),
                                                        Transaction_count bigint, 
                                                        Percentage float)'''

mycursor.execute(create_query_2)
mycon.commit()

df_user = process_data("C:/Users/FCI/OneDrive/Desktop/New folder/Phonepe/pulse/data/aggregated/user/country/india/state/",
                             {'States': [], 'Years': [], 'Quarter': [], 'Brands': [], 'Transaction_count': [], 'Percentage': []},
                             'usersByDevice', 'brand', 'count', 'percentage')
insert_into_mysql("aggregated_user",df_user)

create_query_3= '''Create table if not exists aggregated_insurance (States varchar(200),
                                                        Years int, 
                                                        Quarter int, 
                                                        Transaction_type varchar(200),
                                                        Transaction_count bigint, 
                                                        Transaction_amount bigint)'''

mycursor.execute(create_query_3)
mycon.commit()

create_query_3= '''Create table if not exists aggregated_insurance (States varchar(200),
                                                        Years int, 
                                                        Quarter int, 
                                                        Transaction_type varchar(200),
                                                        Transaction_count bigint, 
                                                        Transaction_amount bigint)'''

mycursor.execute(create_query_3)
mycon.commit()

df_insurance = process_data("C:/Users/FCI/OneDrive/Desktop/New folder/Phonepe/pulse/data/aggregated/insurance/country/india/state/",
                             {'States': [], 'Years': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_count': [], 'Transaction_amount': []},
                             'transactionData', 'name', 'count', 'amount')
insert_into_mysql("aggregated_insurance", df_insurance)

create_query_4= '''Create table if not exists map_transaction (States varchar(200),
                                                        Years int, 
                                                        Quarter int, 
                                                        Districts varchar(200),
                                                        Transaction_count bigint, 
                                                        Transaction_amount bigint)'''

mycursor.execute(create_query_4)
mycon.commit()

df_map_transaction = process_data('C:/Users/FCI/OneDrive/Desktop/New folder/Phonepe/pulse/data/map/transaction/hover/country/india/state/',
                             {'States': [], 'Years': [], 'Quarter': [], 'Districts': [], 'Transaction_count': [], 'Transaction_amount': []},
                             'hoverDataList', 'name', 'metric[0].count', 'metric[0].amount')
insert_into_mysql("map_transaction", df_map_transaction)

create_query_5= '''Create table if not exists map_user (States varchar(200),
                                                        Years int, 
                                                        Quarter int, 
                                                        Districts varchar(200),
                                                        Registered_Users bigint, 
                                                        App_Opens bigint)'''

mycursor.execute(create_query_5)
mycon.commit()

df_map_user = process_data('C:/Users/FCI/OneDrive/Desktop/New folder/Phonepe/pulse/data/map/user/hover/country/india/state/',
                             {'States': [], 'Years': [], 'Quarter': [], 'Districts': [], 'Registered_Users': [], 'App_Opens': []},
                             'hoverData', '0', '1.registeredUsers', '1.appOpens')
insert_into_mysql("map_user", df_transaction)

create_query_6= '''Create table if not exists map_insurance(States varchar(200),
                                                        Years int, 
                                                        Quarter int, 
                                                        Districts varchar(200),
                                                        Transaction_count bigint, 
                                                        Transaction_amount bigint)'''

mycursor.execute(create_query_6)
mycon.commit()

df_map_insurance = process_data("C:/Users/FCI/OneDrive/Desktop/New folder/Phonepe/pulse/data/map/insurance/hover/country/india/state/",
                            {'States': [], 'Years': [], 'Quarter': [], 'Districts': [], 'Transaction_count': [], 'Transaction_amount': []},
                             'hoverDataList', 'name', 'metric[0].count', 'metric[0].amount')
insert_into_mysql("map_insurance", df_map_insurance)

create_query_7= '''Create table if not exists top_insurance(States varchar(200),
                                                            Years int, 
                                                            Quarter int, 
                                                            Pincodes bigint,
                                                            Transaction_count bigint, 
                                                            Transaction_amount bigint)'''

mycursor.execute(create_query_7)
mycon.commit()

df_top_insurance = process_data("C:/Users/FCI/OneDrive/Desktop/New folder/Phonepe/pulse/data/top/insurance/country/india/state/",
                             {'States': [], 'Years': [], 'Quarter': [], 'Pincodes': [], 'Transaction_count': [], 'Transaction_amount': []},
                             'pincodes', 'entityName', 'metric.count', 'metric.amount')
insert_into_mysql("top_insurance", df_top_insurance)

create_query_8= '''Create table if not exists top_transaction(States varchar(200),
                                                            Years int, 
                                                            Quarter int, 
                                                            Pincodes bigint,
                                                            Transaction_count bigint, 
                                                            Transaction_amount bigint)'''

mycursor.execute(create_query_8)
mycon.commit()

df_top_transaction = process_data(
    "C:/Users/FCI/OneDrive/Desktop/New folder/Phonepe/pulse/data/top/transaction/country/india/state/",
    {'States': [], 'Years': [], 'Quarter': [], 'Pincodes': [], 'Transaction_count': [], 'Transaction_amount': []},
    'Pincodes', 'entityName', 'metric.count', 'metric.amount'
)

insert_into_mysql("top_transaction", df_top_transaction)

create_query_9= '''Create table if not exists top_user(States varchar(200),
                                                            Years int, 
                                                            Quarter int, 
                                                            Pincodes bigint,
                                                            Registered_Users bigint)'''

mycursor.execute(create_query_9)
mycon.commit()

df_top_user = process_data(
    "C:/Users/FCI/OneDrive/Desktop/New folder/Phonepe/pulse/data/top/user/country/india/state/",
    {'States': [], 'Years': [], 'Quarter': [], 'Pincodes': [], 'Registered_Users': []},
    'Pincodes', 'name', 'registeredUsers', ''
)

insert_into_mysql("top_user", df_top_user)

connection_string = 'mysql+mysqlconnector://root:12345@localhost/phonepe_data'

# Create an engine
engine = create_engine(connection_string)

query1 = "SELECT * FROM aggregated_transaction"
df1 = pd.read_sql_query(query1, engine)

query2 = "SELECT * FROM aggregated_user"
df2 = pd.read_sql_query(query2, engine)

query3 = "SELECT * FROM aggregated_insurance"
df3 = pd.read_sql_query(query3, engine)

query4 = "SELECT * FROM map_transaction"
df4 = pd.read_sql_query(query4, engine)

query5 = "SELECT * FROM map_user"
df5 = pd.read_sql_query(query5, engine)

query6 = "SELECT * FROM map_insurance"
df6 = pd.read_sql_query(query6, engine)

query7 = "SELECT * FROM top_transaction"
df7 = pd.read_sql_query(query7, engine)


query8 = "SELECT * FROM top_user"
df8 = pd.read_sql_query(query8, engine)

query9 = "SELECT * FROM top_insurance"
df9 = pd.read_sql_query(query9, engine)

mycon.close()

def Transaction_amount_count_Y(df,year): 

    tacy=df[df['Years'] == year]
    tacy.reset_index(drop=True, inplace=True)
    tacyg = tacy.groupby('States')[['Transaction_count','Transaction_amount']].sum()
    tacyg.reset_index(inplace=True)

     
    fig_amount=px.bar(tacyg,x='States',y='Transaction_amount',title =f'{year} Transaction Amount',
                          color_discrete_sequence=px.colors.sequential.Blues,height=1000,width=1000)
    st.plotly_chart(fig_amount)
   
    
    url ='https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson'
    response = requests.get(url)
    data1=json.loads(response.content)
    state_names=[]
    for feature in data1['features']: 
        state_names.append(feature['properties']['ST_NM'])

    state_names.sort()
    fig_india_2=px.choropleth(tacyg,geojson=data1,locations='States',featureidkey='properties.ST_NM',
                            color='Transaction_count',color_continuous_scale='tropic',
                            range_color=(tacyg['Transaction_count'].min(),tacyg['Transaction_count'].max()),
                            hover_name='States',title=f'{year} TRANSACTION COUNT',fitbounds='locations',height=1000,width=1000)
    fig_india_2.update_geos(visible=False)
    st.plotly_chart(fig_india_2)

    return tacy


def Transaction_amount_count_Y_Q(df, quarter, year=None):  # Add optional year argument
    tacy=df[df['Quarter'] == quarter]
    tacy.reset_index(drop=True, inplace=True)
    tacyg = tacy.groupby('States')[['Transaction_count','Transaction_amount']].sum()
    tacyg.reset_index(inplace=True)
   
    fig_amount=px.bar(tacyg,x='States',y='Transaction_amount',
                        title =f"{year if year else df['Years'].min()} Year QUARTER {quarter} TRANSACTION AMOUNT",  # Use year if provided, otherwise df['Years'].min()
                        color_discrete_sequence=px.colors.sequential.Agsunset,height=1000,width=1000)
    st.plotly_chart(fig_amount)
    
    url ='https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson'
    response = requests.get(url)
    data1=json.loads(response.content)
    state_names=[]
    for feature in data1['features']: 
        state_names.append(feature['properties']['ST_NM'])

    state_names.sort()

    fig_india_2=px.choropleth(tacyg,geojson=data1,locations='States',featureidkey='properties.ST_NM',
                            color='Transaction_count',color_continuous_scale='tropic',
                            range_color=(tacyg['Transaction_count'].min(),tacyg['Transaction_count'].max()),
                            hover_name='States',title=f"{tacy['Years'].min()} Year QUARTER {quarter} TRANSACTION COUNT",fitbounds='locations',height=1000,width=1000)
    fig_india_2.update_geos(visible=False)
    st.plotly_chart(fig_india_2)

    return tacy


def Agg_tran_Transaction_type(df,state):

    tacy=df[df['States']== state]
    tacy.reset_index(drop=True, inplace=True)

    tacyg = tacy.groupby('Transaction_type')[['Transaction_count','Transaction_amount']].sum()
    tacyg.reset_index(inplace=True)
    
    col1,col2=st.columns(2)
    with col1: 
        fig_pie_1=px.pie(data_frame=tacyg,names='Transaction_type',values='Transaction_amount',
                        width=500,title= f'{state.upper()} TRANSACTION AMOUNT',hole=0.5,color_discrete_sequence=px.colors.sequential.Viridis)
        st.plotly_chart(fig_pie_1)
    with col2:
        fig_pie_2=px.pie(data_frame=tacyg,names='Transaction_type',values='Transaction_count',
                        width=500,title= f'{state.upper()} TRANSACTION COUNT',hole=0.5,color_discrete_sequence=px.colors.sequential.Blues)
        st.plotly_chart(fig_pie_2)

def Agg_user_plot1(df,year):
    agus=df[df['Years']==year]
    agus.reset_index(drop=True, inplace=True)
    agusg=pd.DataFrame(agus.groupby('Brands')['Transaction_count'].sum())
    agusg.reset_index(inplace=True)

    fig_1_bar=px.bar(agusg,x='Brands',y='Transaction_count', title= f'{year} BRANDS AND TRANSACTION COUNT',
                     width=1000, color_discrete_sequence=px.colors.sequential.Magenta,hover_name='Brands') 
    st.plotly_chart(fig_1_bar)
    
    return agus

def Agg_user_plot2(df,quarter):
    agus_q=df[df['Quarter']==quarter]
    agus_q.reset_index(drop=True, inplace=True)
    agus_qg=pd.DataFrame(agus_q.groupby('Brands')['Transaction_count'].sum())
    agus_qg.reset_index(inplace=True)

    fig_1_bar=px.bar(agus_qg,x='Brands',y='Transaction_count', title= f'{quarter} QUARTER BRANDS AND TRANSACTION COUNT',
                    width=1000, color_discrete_sequence=px.colors.sequential.Greens_r,hover_name='Brands') 
    st.plotly_chart(fig_1_bar)
    return agus_q

def Agg_user_plot3(df,state):
    agusqs=df[df['States']==state]
    agusqs.reset_index(drop=True, inplace=True)
    fig_1_bar=px.bar(agusqs,x='Brands',y='Transaction_count', title= f'{state} BRANDS,TRANSACTION COUNT AND PERCENTAGE',
                width=1000, color_discrete_sequence=px.colors.sequential.algae,hover_data='Percentage') 
    st.plotly_chart(fig_1_bar)
    return agusqs

#Map_insurance District Type
def map_insure_plot_1(df,year,state):
    tacy=df[(df['Years'] == year) & (df['States'] == state)]
    tacy.reset_index(drop=True, inplace=True)
    tacyg = tacy.groupby('Districts')[['Transaction_count','Transaction_amount']].sum()
    tacyg.reset_index(inplace=True)

    fig_bar_1=px.bar(tacyg, x='Transaction_amount', y='Districts', orientation='h', 
                    title=f'{year} {state.upper()} DISTRICT AND TRANSACTION AMOUNT', color_discrete_sequence=px.colors.sequential.Bluyl_r,width=1000, height=1000)
    st.plotly_chart(fig_bar_1)


    fig_bar_2=px.bar(tacyg, x='Transaction_count', y='Districts', orientation='h', 
                    title=f'{year} {state.upper()} DISTRICT AND TRANSACTION COUNT', color_discrete_sequence=px.colors.sequential.Bluyl_r,width=1000, height=1000)
    st.plotly_chart(fig_bar_2)

    return tacy

def map_insure_plot_2(df,quarter):
    miys= df[df['Quarter'] == quarter]
    miysg= miys.groupby("Districts")[["Transaction_count","Transaction_amount"]].sum()
    miysg.reset_index(inplace= True)

    fig_map_pie_1= px.line(miysg, x= "Districts", y= "Transaction_amount",
                            width=1000, height=1000, title= f"{quarter} QUARTER DISTRICT-WISETRANSACTION AMOUNT",
                            color_discrete_sequence= px.colors.sequential.Mint_r)
    st.plotly_chart(fig_map_pie_1)

    fig_map_pie_1= px.line(miysg, x= "Districts", y= "Transaction_count",
                            width=1000, height=1000, title= f"{quarter} QUARTER DISTRICT-WISE TRANSACTION COUNT",
                            color_discrete_sequence= px.colors.sequential.Oranges_r)
    
    st.plotly_chart(fig_map_pie_1)

def map_user_plot1(df,year):
    muy=df[df['Years']==year]
    muy.reset_index(drop=True, inplace=True)
    muy_g=muy.groupby('States')[['Registered_Users','App_Opens']].sum()
    muy_g.reset_index(inplace=True)

    fig_1_line=px.line(muy_g,x='States',y=['Registered_Users','App_Opens'], title= 'REGISTERED USERS AND APP OPENS',
                        width=1000,height=800,markers=True,hover_name='States',
                         color_discrete_map={'Registered_Users': 'blue', 'App_Opens': 'red'},
                         labels={'Registered_Users': 'Registered Users', 'App_Opens': 'App Opens'},
                        )
    st.plotly_chart(fig_1_line)

    return muy

def map_user_plot_2(df, quarter):
    muyq= df[df["Quarter"] == quarter]
    muyq.reset_index(drop= True, inplace= True)
    muyqg= muyq.groupby("States")[["Registered_Users", "App_Opens"]].sum()
    muyqg.reset_index(inplace= True)

    fig_map_user_plot_1= px.line(muyqg, x= "States", y= ["Registered_Users","App_Opens"], markers= True,
                                title= f"{df['Years'].min()}, {quarter} QUARTER REGISTERED USER AND APPOPENS",
                                width= 1000,height=800,color_discrete_sequence= px.colors.sequential.Rainbow_r)
    st.plotly_chart(fig_map_user_plot_1)

    return muyq



def map_user_plot3(df, states):  
    muyqs = df[df['States'] == states]
    muyqs.reset_index(drop=True, inplace=True)
    fig_map_user_bar1 = px.bar(muyqs, x='Registered_Users', y='Districts', orientation='h', title='REGISTERED USER',
                               height=800, color_discrete_sequence=px.colors.sequential.Greens_r)
    st.plotly_chart(fig_map_user_bar1)

    fig_map_user_bar2 = px.bar(muyqs, x='App_Opens', y='Districts', orientation='h', title='APP OPENS',
                               height=800, color_discrete_sequence=px.colors.sequential.Darkmint)
    st.plotly_chart(fig_map_user_bar2)

def top_transaction_plot(df, state):
    muyqs = df[df['States'] == state]
    muyqs.reset_index(drop=True, inplace=True)

    col1, col2 = st.columns(2)
    with col1:
        fig_pie_1 = px.pie(data_frame=muyqs, names='Pincodes', values='Transaction_amount',
                           width=500, title=f'{state.upper()} TRANSACTION AMOUNT',
                           hole=0.5, color_discrete_sequence=px.colors.sequential.Viridis)
        st.plotly_chart(fig_pie_1)
    with col2:
        fig_pie_2 = px.pie(data_frame=muyqs, names='Pincodes', values='Transaction_count',
                           width=500, title=f'{state.upper()} TRANSACTION AMOUNT',
                           hole=0.5, color_discrete_sequence=px.colors.sequential.Viridis)
        st.plotly_chart(fig_pie_2)

    # Return the modified DataFrame
    return muyqs

def top_transaction_plot_1(df, year):
    muyqs = df[df['Years'] == year]
    muyqs.reset_index(drop=True, inplace=True)

    fig_pie_1 = px.bar(data_frame=muyqs, x='Pincodes', y='Transaction_amount',
                        width=1000, title=f'{year} TRANSACTION AMOUNT',
                         color_discrete_sequence=px.colors.sequential.Viridis)
    st.plotly_chart(fig_pie_1)

    fig_pie_2 = px.bar(data_frame=muyqs, x='Pincodes', y='Transaction_count',
                        width=1000, title=f'{year} TRANSACTION AMOUNT',
                        color_discrete_sequence=px.colors.sequential.Viridis)
    st.plotly_chart(fig_pie_2)

    # Return the modified DataFrame
    return muyqs


def top_user_plot_1(df,year):
    tuy= df[df["Years"] == year]
    tuy.reset_index(drop= True, inplace= True)

    tuyg= pd.DataFrame(tuy.groupby(["States","Quarter"])["Registered_Users"].sum())
    tuyg.reset_index(inplace= True)

    fig_top_plot_1= px.bar(tuyg, x= "States", y= "Registered_Users", barmode= "group", color= "Quarter",
                            width=1000, height= 800, color_continuous_scale= px.colors.sequential.Burgyl)
    st.plotly_chart(fig_top_plot_1)

    return tuy

def top_user_plot_2(df,state):
    tuys= df[df["States"] == state]
    tuys.reset_index(drop= True, inplace= True)

    tuysg= pd.DataFrame(tuys.groupby("States")["Registered_Users"].sum())
    tuysg.reset_index(inplace= True)

    fig_top_plot_1= px.bar(tuys, x= "States", y= "Registered_Users",barmode= "group",
                           width=1000, height= 800,color= "Registered_Users",hover_data="Pincodes",
                            color_continuous_scale= px.colors.sequential.Magenta)
    st.plotly_chart(fig_top_plot_1)

def ques1():
    brand= df2[["Brands","Transaction_count"]]
    brand1= brand.groupby("Brands")["Transaction_count"].sum().sort_values(ascending=False)
    brand2= pd.DataFrame(brand1).reset_index()

    fig_brands= px.bar(brand2, x= "Transaction_count", y= "Brands", color_discrete_sequence=px.colors.sequential.speed,
                       title= "Top Mobile Brands of Transaction_count")
    return st.plotly_chart(fig_brands)

def ques2():
    lt= df1[["States", "Transaction_amount"]]
    lt1= lt.groupby("States")["Transaction_amount"].sum().sort_values(ascending= True)
    lt2= pd.DataFrame(lt1).reset_index().head(10)

    fig_lts= px.bar(lt2, x= "States", y= "Transaction_amount",title= "LOWEST TRANSACTION AMOUNT and STATES",
                    color_discrete_sequence= px.colors.sequential.Oranges_r)
    return st.plotly_chart(fig_lts)

def ques3():
    htd= df4[["Districts", "Transaction_amount"]]
    htd1= htd.groupby("Districts")["Transaction_amount"].sum().sort_values(ascending=False)
    htd2= pd.DataFrame(htd1).head(10).reset_index()

    fig_htd= px.pie(htd2, values= "Transaction_amount", names= "Districts", title="TOP 10 DISTRICTS OF HIGHEST TRANSACTION AMOUNT",
                    color_discrete_sequence=px.colors.sequential.Emrld_r)
    return st.plotly_chart(fig_htd)

def ques4():
    htd= df4[["Districts", "Transaction_amount"]]
    htd1= htd.groupby("Districts")["Transaction_amount"].sum().sort_values(ascending=True)
    htd2= pd.DataFrame(htd1).head(10).reset_index()

    fig_htd= px.pie(htd2, values= "Transaction_amount", names= "Districts", title="TOP 10 DISTRICTS OF LOWEST TRANSACTION AMOUNT",
                    color_discrete_sequence=px.colors.sequential.Greens_r)
    return st.plotly_chart(fig_htd)


def ques5():
    sa= df5[["States", "App_Opens"]]
    sa1= sa.groupby("States")["App_Opens"].sum().sort_values(ascending=False)
    sa2= pd.DataFrame(sa1).reset_index().head(10)

    fig_sa= px.bar(sa2, x= "States", y= "App_Opens", title="Top 10 States With AppOpens",
                color_discrete_sequence= px.colors.sequential.deep_r)
    return st.plotly_chart(fig_sa)

def ques6():
    sa= df5[["States", "App_Opens"]]
    sa1= sa.groupby("States")["App_Opens"].sum().sort_values(ascending=True)
    sa2= pd.DataFrame(sa1).reset_index().head(10)

    fig_sa= px.bar(sa2, x= "States", y= "App_Opens", title="lowest 10 States With AppOpens",
                color_discrete_sequence= px.colors.sequential.dense_r)
    return st.plotly_chart(fig_sa)

def ques7():
    stc= df1[["States", "Transaction_count"]]
    stc1= stc.groupby("States")["Transaction_count"].sum().sort_values(ascending=True)
    stc2= pd.DataFrame(stc1).reset_index()

    fig_stc= px.bar(stc2, x= "States", y= "Transaction_count", title= "STATES WITH LOWEST TRANSACTION COUNT",
                    color_discrete_sequence= px.colors.sequential.Jet_r)
    return st.plotly_chart(fig_stc)

def ques8():
    stc= df1[["States", "Transaction_count"]]
    stc1= stc.groupby("States")["Transaction_count"].sum().sort_values(ascending=False)
    stc2= pd.DataFrame(stc1).reset_index()

    fig_stc= px.bar(stc2, x= "States", y= "Transaction_count", title= "STATES WITH HIGHEST TRANSACTION COUNT",
                    color_discrete_sequence= px.colors.sequential.Magenta_r)
    return st.plotly_chart(fig_stc)

def ques9():
    ht= df1[["States", "Transaction_amount"]]
    ht1= ht.groupby("States")["Transaction_amount"].sum().sort_values(ascending= False)
    ht2= pd.DataFrame(ht1).reset_index().head(10)

    fig_lts= px.bar(ht2, x= "States", y= "Transaction_amount",title= "HIGHEST TRANSACTION AMOUNT and STATES",
                    color_discrete_sequence= px.colors.sequential.Oranges_r)
    return st.plotly_chart(fig_lts)

def ques10():
    dt= df4[["Districts", "Transaction_amount"]]
    dt1= dt.groupby("Districts")["Transaction_amount"].sum().sort_values(ascending=True)
    dt2= pd.DataFrame(dt1).reset_index().head(50)

    fig_dt= px.bar(dt2, x= "Districts", y= "Transaction_amount", title= "DISTRICTS WITH LOWEST TRANSACTION AMOUNT",
                color_discrete_sequence= px.colors.sequential.Mint_r)
    return st.plotly_chart(fig_dt)

#streamlit code

# Dropdown to select data
mycon = mysql.connector.connect(host='localhost', user='root', password='12345',database='phonepe_data')
mycursor = mycon.cursor()

st.set_page_config(layout="wide")

option = option_menu(None,
                       options = ["Home","Analysis","Insights",],
                       icons = ["house","toggles","at"],
                       default_index=0,
                       orientation="horizontal",
                       styles={"container": {"width": "100%"},
                               "icon": {"color": "white", "font-size": "24px"},
                               "nav-link": {"font-size": "24px", "text-align": "center", "margin": "-2px"},
                               "nav-link-selected": {"background-color": "#6F36AD"}})


        
        
if option == "Home":
    col1,col2 = st.columns(2)
    with col1:
        st.image(Image.open("C:/Users/FCI/OneDrive/Desktop/New folder/Phonepe/phonepe.jpg"), width=500)
    with col2:
        st.title(':Purple[PHONEPE PULSE DATA VISUALISATION]')
        st.subheader(':Purple[Phonepe Pulse]:')
        st.write('PhonePe Pulse is a feature offered by the Indian digital payments platform called PhonePe.PhonePe Pulse provides users with insights and trends related to their digital transactions and usage patterns on the PhonePe app.')
        st.subheader(':Purple[Phonepe Pulse Data Visualisation]:')
        st.write('Data visualization refers to the graphical representation of data using charts, graphs, and other visual elements to facilitate understanding and analysis in a visually appealing manner.'
                 'The goal is to extract this data and process it to obtain insights and information that can be visualized in a user-friendly manner.')
        st.markdown("[Githublink](https://github.com/Mukesh-babu)")
    st.write("---")

if option == "Analysis":
    tab1,tab2,tab3 = st.tabs(['Aggregated Analyis','Map Analysis','Top Analysis'])
    
    with tab1:
        method=st.selectbox('Select the method',['Aggregated Insurance Analysis','Transaction Analysis','Aggregated User Analysis'])

        if method =='Transaction Analysis':
            col1,col2,col3=st.columns(3)
            with col1: 
                years=st.selectbox('Select the year',df1['Years'].unique())
            with col2: 
                quarters = st.selectbox('Select the Quarter',df1['Quarter'].unique(),key='unique_key_for_quarters')
            with col3:
                states = st.selectbox('Select the State:',df1['States'].unique())
            Agg_tran_Transaction_type(df1,states)
            Agg_Trans_tac_Y_Q = Transaction_amount_count_Y_Q(df1,quarters)

        elif method == 'Aggregated Insurance Analysis':
            col1,col2=st.columns(2)
            with col1: 
                years=st.selectbox('Select the year',df3['Years'].unique()) 
            tac_Y=Transaction_amount_count_Y(df3,years)
            with col2: 
                quarters = st.selectbox('Select the Quarter',tac_Y['Quarter'].unique(),key='unique_key_for_quarters')
            Transaction_amount_count_Y_Q(tac_Y,quarters)

        elif method=='Aggregated User Analysis':

            col1,col2,col3=st.columns(3)
            with col1: 
                years=st.selectbox('Select the year',df2['Years'].unique())
            with col2: 
                quarters = st.selectbox('Select the Quarter',df2['Quarter'].unique(),key='unique_key_for_quarters')
            with col3:
                states = st.selectbox('Select the State:',df2['States'].unique())

            Agg_user_Y=Agg_user_plot1(df2,years)

            Agg_user_Y_Q = Agg_user_plot2(Agg_user_Y,quarters)

            Agg_user_plot3(Agg_user_Y_Q,states)

        
    with tab2: 
        method_1=st.selectbox('Select the method',['Map Insurance Analysis','Map Transaction Analysis','Map User Analysis'])

        if method_1 == 'Map Insurance Analysis':

            col1,col2,col3=st.columns(3)
            with col1: 
                years_mi=st.selectbox('Select the year',df6['Years'].unique(),key='unique_key_for_years_mi')
            with col2: 
                quarter_mi = st.selectbox('Select the Quarter',df6['Quarter'].unique(),key='unique_key_for_quarters_mi')
            with col3:
                states_mi = st.selectbox('Select the State:',df6['States'].unique(),key='unique_key_for_states_mi')

            Map_insur_tac_Y=map_insure_plot_1(df6,years,states_mi)
            Map_insur_tac_Y_Q = map_insure_plot_2(Map_insur_tac_Y,quarter_mi)
            

            
        elif method_1=='Map Transaction Analysis':

            col1,col2,col3=st.columns(3)
            with col1: 
                years_mt=st.selectbox('Select the year',df4['Years'].unique(),key='unique_key_for_years_mt')
            with col2: 
                quarters_mt = st.selectbox('Select the Quarter',df4['Quarter'].unique(),key='unique_key_for_quarters_mt')
            with col3:
                states_mt = st.selectbox('Select the State:',df4['States'].unique(),key='unique_key_for_states_mt')

            
            Map_trans_tac_Y=Transaction_amount_count_Y(df4,years)
            map_insure_plot_1(Map_trans_tac_Y,years, states_mt)
            Map_trans_tac_Y_Q = Transaction_amount_count_Y_Q(Map_trans_tac_Y,quarters_mt)
            map_insure_plot_2(Map_trans_tac_Y_Q ,quarters_mt)

            
        elif method_1=='Map User Analysis':
        
            col1,col2,col3=st.columns(3)
            with col1: 
                years_mu=st.selectbox('Select the year',df5['Years'].unique(),key='unique_key_for_years_mu')
            with col2: 
                quarters_mu = st.selectbox('Select the Quarter',df5['Quarter'].unique(),key='unique_key_for_quarters_mu')
            with col3:
                states_mu = st.selectbox('Select the State:',df5['States'].unique(),key='unique_key_for_states_mu')
            
            Map_user_Y_Q = map_user_plot_2(df5,quarters_mu)
            map_user_plot3(Map_user_Y_Q,states_mu)
        



    with tab3: 
        method_2=st.radio('Select the method',['Top Insurance Analysis','Top Transaction Analysis','Top User Analysis'])

        if method_2 == 'Top Insurance Analysis':
            col1,col2=st.columns(2)
            with col1: 
                years_t1=st.selectbox('Select the year',df9['Years'].unique(),key='unique_key_for_years_t1')
            with col2: 
               quarters_t1  = st.selectbox('Select the Quarter',df9['Quarter'].unique(),key='unique_key_for_quarters_t1')
            df_top_insur_Y= Transaction_amount_count_Y(df9,years_t1)
            df_top_insur_Y_Q = Transaction_amount_count_Y_Q(df_top_insur_Y,quarters_t1)

    

        elif method_2=='Top Transaction Analysis':
            col1,col2=st.columns(2)
            with col1: 
                years_t4=st.selectbox('Select the year',df7['Years'].unique(),key='unique_key_for_years_t4')
            with col2: 
               states_t4 = st.selectbox('Select the State:',df7['States'].unique(),key='unique_key_for_states_t4')
    
            df_top_tran_Y=top_transaction_plot_1(df7,years_t4) 
            df_top_trans_Y_Q = top_transaction_plot(df_top_tran_Y,states_t4)


        

            

        elif method_2=='Top User Analysis':     
            col1,col2=st.columns(2)
            with col1: 
                years_t3=st.selectbox('Select the year',df9['Years'].unique(),key='unique_key_for_years_t3')
            with col2: 
                states_t3 = st.selectbox('Select the State_top',df9['States'].unique(),key='unique_key_for_states_t3') 

            df_top_user_Y= top_user_plot_1(df8,years_t3)
            df_top_user_Y_S= top_user_plot_2(df_top_user_Y,states_t3)






if option == "Insights":
    ques= st.selectbox("**Select the Question**",('Top Brands Of Mobiles Used','States With Lowest Trasaction Amount',
                                  'Districts With Highest Transaction Amount','Top 10 Districts With Lowest Transaction Amount',
                                  'Top 10 States With AppOpens','Least 10 States With AppOpens','States With Lowest Trasaction Count',
                                 'States With Highest Trasaction Count','States With Highest Trasaction Amount',
                                 'Top 50 Districts With Lowest Transaction Amount'))
    
    if ques=="Top Brands Of Mobiles Used":
        ques1()

    elif ques=="States With Lowest Trasaction Amount":
        ques2()

    elif ques=="Districts With Highest Transaction Amount":
        ques3()

    elif ques=="Top 10 Districts With Lowest Transaction Amount":
        ques4()

    elif ques=="Top 10 States With AppOpens":
        ques5()

    elif ques=="Least 10 States With AppOpens":
        ques6()

    elif ques=="States With Lowest Trasaction Count":
        ques7()

    elif ques=="States With Highest Trasaction Count":
        ques8()

    elif ques=="States With Highest Trasaction Amount":
        ques9()

    elif ques=="Top 50 Districts With Lowest Transaction Amount":
        ques10()






