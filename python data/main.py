import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt
import seaborn as sns

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
    
    

    print("Columns in DataFrame:", df.columns)

    # Visualization 1: Bar Chart 
    df_sorted = df.sort_values(by='Revenue', ascending=False).head(10)
    plt.bar(df_sorted['Company'], df_sorted['Revenue'], color='skyblue')
    plt.xlabel('Company Name')
    plt.ylabel('Revenue')
    plt.title('Top 10 Companies by Revenue')
    plt.xticks(rotation=90)
    plt.tight_layout()
    plt.show()

    # Visualization 2: Pie Chart 
    df_industry_revenue = df.groupby('Industry')['Revenue'].sum().reset_index()
    plt.pie(df_industry_revenue['Revenue'], labels=df_industry_revenue['Industry'])
    plt.title('Revenue Distribution by Industry')
    plt.tight_layout()
    plt.show()

    # Visualization 3: Scatter 
    sns.scatterplot(data=df, x='Revenue', y='Profits', hue='Industry')
    plt.xlabel('Revenue')
    plt.ylabel('Profit')
    plt.title('Revenue vs. Profit by Industry')
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    # Visualization 4: Histogram 
    plt.hist(df['Profits'], bins=20, color='green', edgecolor='black')
    plt.xlabel('Profits')
    plt.ylabel('Frequency')
    plt.title('Distribution of Company Profits')
    plt.grid(True)
    plt.tight_layout()
    plt.show()

    # Visualization 4: Box 
    df.boxplot(column='Revenue', by='Industry', grid=True)
    plt.xlabel('Industry')
    plt.ylabel('Revenue')
    plt.title('Box Plot of Revenue by Industry')
    plt.suptitle('REVENUE')
    plt.show()
except mysql.connector.Error as err:
    print("Error:", err)

finally:
    if connection.is_connected():
        cursor.close()
        connection.close()
        print("Connection closed.")
