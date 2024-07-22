import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt
import plotly.express as px

config = {
    'user': 'root',
    'password': 'Romanreigns1@',
    'host': 'localhost',
    'database': 'dataenv',
}

file_path = 'fortune1000.csv'

try:
    connection = mysql.connector.connect(**config)
    cursor = connection.cursor()
    
    if connection.is_connected():
        print("Successfully connected to the database")

    df = pd.read_csv(file_path)
    
    # Verify column names
    print("Columns in DataFrame:", df.columns)

    # Visualization 1: Top 10 Companies by Revenue
    df_sorted = df.sort_values(by='Revenue', ascending=False).head(10)
    plt.figure(figsize=(12, 8))
    plt.bar(df_sorted['Company'], df_sorted['Revenue'], color='skyblue')
    plt.xlabel('Company Name')
    plt.ylabel('Revenue')
    plt.title('Top 10 Companies by Revenue')
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.show()

    # Visualization 2: Pie Chart of Revenue Distribution by Industry
    df_industry_revenue = df.groupby('Industry')['Revenue'].sum().reset_index()
    fig = px.pie(df_industry_revenue, names='Industry', values='Revenue', 
                title='Revenue Distribution by Industry',
                labels={'Industry': 'Industry', 'Revenue': 'Revenue'})
    fig.update_traces(textinfo='percent+label')
    fig.show()

    # Visualization 3: Scatter Plot of Revenue vs. Profit by Industry
    fig = px.scatter(df, x='Revenue', y='Profits', color='Industry',
                     title='Revenue vs. Profit by Industry',
                     labels={'Revenue': 'Revenue', 'Profits': 'Profit'})
    fig.show()

except mysql.connector.Error as err:
    print("Error:", err)

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("Connection closed.")
