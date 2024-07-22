import matplotlib.pyplot as plt
from main import df 

df_sorted = df.sort_values(by='revenue', ascending=False).head(10)

plt.figure(figsize=(12, 8))
plt.bar(df_sorted['company_name'], df_sorted['revenue'], color='skyblue')
plt.xlabel('Company Name')
plt.ylabel('Revenue')
plt.title('Top 10 Companies by Revenue')
plt.xticks(rotation=90)
plt.tight_layout()
plt.show()