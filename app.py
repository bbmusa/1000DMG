

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine
import toml
config = toml.load("config.toml")

def login_db():
    try:
        # AWS RDS configuration
        rds_host = config['HOST']
        rds_user = config['DB_USER']
        rds_password = config['PASS']
        rds_db_name = 'ema1000'
        rds_port = 3306


        # Create the connection string
        connection_string = f'mysql+pymysql://{rds_user}:{rds_password}@{rds_host}:{rds_port}/{rds_db_name}'

        # Create the SQLAlchemy engine
        engine = create_engine(connection_string)
        print("Connection to the database was successful!")
        return engine

    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None


def read_all_data(engine, table_name):
    try:
        # Query the table and load data into a DataFrame
        query = f"SELECT * FROM {table_name};"
        df = pd.read_sql(query, con=engine)

        if df.empty:
            print(f"No data found in the table: {table_name}")
        else:
            print(f"Data successfully read from table: {table_name}")
            return df

    except Exception as e:
        print(f"Error reading data from the database: {e}")
        return None

def fetch_data():
    engine = login_db()
    data = pd.DataFrame()
    if engine:
        table_name = "analysis_results"
        data = read_all_data(engine, table_name)
    return data

def count_and_plot_symbols(df, days_range=0):
    # Convert date column to datetime if it's not already
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    latest_date = df['date'].max()

    start_date = latest_date - pd.Timedelta(days=days_range)
    df_range = df[df['date'] > start_date]
    range_count_df = df_range.groupby(['date', 'symbol', 'sector']).size().reset_index(name='count')

    # Count symbols for the previous day
    prev_day = latest_date - pd.Timedelta(days=1)
    prev_day_df = df[df['date'] == prev_day]
    prev_day_count_df = prev_day_df.groupby(['symbol', 'sector']).size().reset_index(name='count')
    prev_day_count_df['date'] = prev_day
    range_count_df = range_count_df.sort_values('count', ascending=False)
    prev_day_count_df = prev_day_count_df.sort_values('count', ascending=False)

    # Sort dataframes by count in descending order

    # Filter the original dataframe to include only the top 20 symbols
    # top_symbols = range_count_df.groupby('symbol')['count'].sum().nlargest(20).index
    # top_range_count_df = range_count_df[range_count_df['symbol'].isin(top_symbols)]
    # top_range_count_df['avg_count'] = top_range_count_df.groupby('date')['count'].transform('mean')
    # fig_range = px.line(top_range_count_df, x='date', y='avg_count', color='symbol',
    #                     title=f'Symbol Count for Top 20 Symbols Over the Past {days_range} Days')


    # Display plots in Streamlit
    # st.plotly_chart(fig_range)


    fig_prev_day = px.bar(prev_day_count_df, x='symbol', y='count',
                          title=f'Symbol Count for Previous Day ({prev_day.date()})')
    st.plotly_chart(fig_prev_day)

    # Create and display tables
    st.subheader(f"Top 10 Stocks by Count (Previous Day - {prev_day.date()})")
    st.table(prev_day_count_df[['symbol', 'sector', 'count']])

    st.subheader(f"Top 10 Stocks by Count (Past {days_range} Days)")
    top_10_range = (
        range_count_df.groupby(['symbol', 'sector'])['count']
        .sum()
        .sort_values(ascending=False)
        .reset_index()
    )
    st.table(top_10_range)

    # Top sectors
    st.subheader(f"Top Sectors by Count (Previous Day - {prev_day.date()})")
    top_sectors_prev = prev_day_count_df.groupby('sector')['count'].sum().sort_values(ascending=False).reset_index()
    st.table(top_sectors_prev)

    st.subheader(f"Top Sectors by Count (Past {days_range} Days)")
    top_sectors_range = range_count_df.groupby('sector')['count'].sum().sort_values(ascending=False).reset_index()
    st.table(top_sectors_range)

def main():
    st.title('1000 EMA DMG')
    df = fetch_data()
    days_range = st.slider('Select number of days for analysis', min_value=1,max_value=10, value=10)
    count_and_plot_symbols(df, days_range)

if __name__ == "__main__":
    main()