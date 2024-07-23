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
    
    cursor.execute("SHOW TABLES LIKE 'fortune1000'")
    result = cursor.fetchone()
    
    if not result:
        create_table_query = '''
        CREATE TABLE fortune1000 (
            Rank INT,
            Company VARCHAR(255),
            Revenue BIGINT,
            Profits BIGINT,
            Industry VARCHAR(255),
            PRIMARY KEY (Rank, Company)
        )
        '''
        cursor.execute(create_table_query)
        print("Table 'fortune1000' created.")

    for index, row in df.iterrows():
        select_query = 'SELECT * FROM fortune1000 WHERE Rank = %s AND Company = %s'
        cursor.execute(select_query, (row['Rank'], row['Company']))
        existing_record = cursor.fetchone()

        if existing_record:
            update_query = '''
            UPDATE fortune1000 
            SET Revenue = %s, Profits = %s, Industry = %s 
            WHERE Rank = %s AND Company = %s
            '''
            values = (row['Revenue'], row['Profits'], row['Industry'], row['Rank'], row['Company'])
            print(f"Executing update query: {update_query} with values: {values}")
            cursor.execute(update_query, values)
        else:
            insert_query = '''
            INSERT INTO fortune1000 (Rank, Company, Revenue, Profits, Industry) 
            VALUES (%s, %s, %s, %s, %s)
            '''
            print(f"Executing insert query: {insert_query} with values: {(row['Rank'], row['Company'], row['Revenue'], row['Profits'], row['Industry'])}")
            cursor.execute(insert_query, (row['Rank'], row['Company'], row['Revenue'], row['Profits'], row['Industry']))


    connection.commit()
    print("Data updated successfully.")

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

    # Visualization 5: Box Plot
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
