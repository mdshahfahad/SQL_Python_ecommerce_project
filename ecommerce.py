#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install mysql-connector-python


# In[1]:


import pandas as pd
import mysql.connector
import os

# List of CSV files and their corresponding table names
csv_files = [
    ('customers.csv', 'customers'),
    ('orders.csv', 'orders'),
    ('sellers.csv', 'sellers'),
    ('products.csv', 'products'),
    ('geolocation.csv', 'geolocation'),
    ('order_items.csv', 'order_items'),
    ('payments.csv', 'payments')  # Added payments.csv for specific handling
]

# Connect to the MySQL database
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='root',
    database='ecommerce'
)
cursor = conn.cursor()

# Folder containing the CSV files
folder_path = 'F:/Md Shah Fahad/Project July/Tables'

def get_sql_type(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INT'
    elif pd.api.types.is_float_dtype(dtype):
        return 'FLOAT'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'DATETIME'
    else:
        return 'TEXT'

for csv_file, table_name in csv_files:
    file_path = os.path.join(folder_path, csv_file)
    
    # Read the CSV file into a pandas DataFrame
    df = pd.read_csv(file_path)
    
    # Replace NaN with None to handle SQL NULL
    df = df.where(pd.notnull(df), None)
    
    # Debugging: Check for NaN values
    print(f"Processing {csv_file}")
    print(f"NaN values before replacement:\n{df.isnull().sum()}\n")

    # Clean column names
    df.columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in df.columns]

    # Generate the CREATE TABLE statement with appropriate data types
    columns = ', '.join([f'`{col}` {get_sql_type(df[col].dtype)}' for col in df.columns])
    create_table_query = f'CREATE TABLE IF NOT EXISTS `{table_name}` ({columns})'
    cursor.execute(create_table_query)

    # Insert DataFrame data into the MySQL table
    for _, row in df.iterrows():
        # Convert row to tuple and handle NaN/None explicitly
        values = tuple(None if pd.isna(x) else x for x in row)
        sql = f"INSERT INTO `{table_name}` ({', '.join(['`' + col + '`' for col in df.columns])}) VALUES ({', '.join(['%s'] * len(row))})"
        cursor.execute(sql, values)

    # Commit the transaction for the current CSV file
    conn.commit()

# Close the connection
conn.close()


# In[2]:


import matplotlib.pyplot as plt
import seaborn as sns


# In[3]:


db = mysql.connector.connect(host="localhost",username="root",password="root",database="ecommerce")
cur=db.cursor()


# # Basic Queries
# 

# ## 1. List all unique cities where customers are located.

# In[21]:


query = """select distinct(customer_city) from customers"""
cur.execute(query)
data=cur.fetchall()
data
df=pd.DataFrame(data, columns = ["Cities"])
df


# ## 2.Count the number of orders placed in 2017.
# 

# In[22]:


query = """select count(order_id) from orders where year(order_purchase_timestamp)=2017"""
cur.execute(query)
data=cur.fetchall()
data
df=pd.DataFrame(data, columns = ["Total Orders In 2017"])
df


# ## 3.Find the total sales per category.

# In[23]:


query = """select products.product_category as category,
round(sum(payments.payment_value),2) as sales
from products join order_items
on products.product_id= order_items.product_id
join payments on payments.order_id=order_items.order_id
group by category """
cur.execute(query)
data=cur.fetchall()
data
df=pd.DataFrame(data, columns = ["Category","Sales_Value"])
df


# ## 4.Calculate the percentage of orders that were paid in installments.

# In[24]:


query = """ select (sum(case when payment_installments >=1 then 1 else 0 end ))/count(*)*100 from payments """
cur.execute(query)
data=cur.fetchall()
data
df=pd.DataFrame(data, columns = ["Percentage of Orders paid in Installments"])
df


# ## 5.Count the number of customers from each state. 

# In[8]:


query = """ select customer_state, count(customer_id) from customers group by customer_state """
cur.execute(query)
data=cur.fetchall()
data
df=pd.DataFrame(data, columns = ["States","Customer_Count"])
df=df.sort_values(by='Customer_Count', ascending=False)
plt.figure(figsize=(15,6))
plt.bar(df["States"],df["Customer_Count"])
plt.title("States Vise Customer Count")
plt.xlabel("States")
plt.ylabel("Customer Counts")
plt.xticks(rotation=90)
plt.show()


# # Intermediate Queries

# ## 1.Calculate the number of orders per month in 2018.

# In[9]:


query= """ select monthname(order_purchase_timestamp) month, count(order_id) from orders
where year(order_purchase_timestamp)=2018
group by month
"""

cur.execute(query)
data=cur.fetchall()
data
df=pd.DataFrame(data, columns=["Months", "Count"])
ax=sns.barplot(x=df["Months"],y=df["Count"], data=df)
plt.xticks(rotation=45)
ax.bar_label(ax.containers[0])
plt.title("Number of Orders per Month in 2018")
plt.show()


# ## 2.Find the average number of products per order, grouped by customer city.

# In[18]:


query= """ 

with count_per_order as 
(select orders.order_id, orders.customer_id, count(order_items.order_id) as oc
from orders join order_items on orders.order_id=order_items.order_id
group by orders.order_id, orders.customer_id)

select customers.customer_city, round(avg(count_per_order.oc),2) average_orders 
from customers join count_per_order
on customers.customer_id=count_per_order.customer_id
group by customers.customer_city order by average_orders desc

"""

cur.execute(query)
data=cur.fetchall()
data


df=pd.DataFrame(data, columns=["City", "Average Count"])
df.head(10)


# ## 3. Calculate the percentage of total revenue contributed by each product category.

# In[20]:


query = """select products.product_category as category,
round((sum(payments.payment_value)/(select sum(payment_value) from payments))*100,2) as sales_percentage
from products join order_items
on products.product_id= order_items.product_id
join payments on payments.order_id=order_items.order_id
group by category order by sales_percentage desc """
cur.execute(query)
data=cur.fetchall()
data
df=pd.DataFrame(data, columns = ["Category","Sales_Percentage"])
df


# ## 4. Identify the correlation between product price and the number of times a product has been purchased.

# In[30]:


import numpy as np

query = """select products.product_category as category,
round(avg(order_items.price),2) as ave_price,
count(order_items.product_id) order_count
from products join order_items
on products.product_id=order_items.product_id
group by products.product_category
"""
cur.execute(query)
data=cur.fetchall()
data
df=pd.DataFrame(data, columns = ["Category","Avg_Price","Order_count"])
arr1=df["Avg_Price"]
arr2=df["Order_count"]
np.corrcoef([arr1,arr2])


# ## 5. Calculate the total revenue generated by each seller, and rank them by revenue.

# In[44]:


query = """select order_items.seller_id as Seller ,
round(sum(payments.payment_value),2) as Revenue from order_items
join payments on order_items.order_id=payments.order_id
group by seller_id order by Revenue desc limit 5;
"""
cur.execute(query)
data=cur.fetchall()
data
df=pd.DataFrame(data, columns = ["Seller","Revenue"])
df.head()
sns.barplot(x="Seller",y="Revenue",data=df)
plt.xticks(rotation=90)
plt.show()


# # Advanced Queries

# ## 1. Calculate the moving average of order values for each customer over their order history.

# In[50]:


query = """select customer_id,order_purchase_timestamp, payment, 
avg(payment) over(partition by customer_id order by order_purchase_timestamp 
rows between 2 preceding and current row) as mov_avg from
(select orders.customer_id, orders.order_purchase_timestamp, payments.payment_value as payment
from payments join orders on payments.order_id=orders.order_id) as a ;
"""
cur.execute(query)
data=cur.fetchall()
data
df=pd.DataFrame(data, columns = ["Customer ID","Time","Payment", "Moving Avg"])
df


# ## 2. Calculate the cumulative sales per month for each year.

# In[51]:


query = """
select years,months,sales,round(sum(sales) over(order by years, months),2) as Cumm_Sales from 
(select year(orders.order_purchase_timestamp) as years,
month(orders.order_purchase_timestamp) as months,
round(sum(payments.payment_value),2) as sales from orders 
join payments on orders.order_id=payments.order_id
group by years, months order by years, months) as a;

"""
cur.execute(query)
data=cur.fetchall()
data
df=pd.DataFrame(data, columns = ["Year","Month","Sales_per_Month", "Cumm_Sales"])
df


# ## 3. Calculate the year-over-year growth rate of total sales.

# In[53]:


query = """
with a as(select year(orders.order_purchase_timestamp) as years,
round(sum(payments.payment_value),2) as sales from orders 
join payments on orders.order_id=payments.order_id
group by years order by years asc)
select years, round((((sales-lag(sales,1) over(order by years))/lag(sales,1) over(order by years))*100),2) from a;
"""
cur.execute(query)
data=cur.fetchall()
data
df=pd.DataFrame(data, columns = ["Year","Growth Percentage"])
df


# ## 4. Calculate the retention rate of customers, defined as the percentage of customers who make another purchase within 6 months of their first purchase.

# In[55]:


query = """

with a as (select customer_id, min(order_purchase_timestamp) as first_order from orders
group by customer_id),

b as (select a.customer_id, count(distinct order_purchase_timestamp) from a
join orders on a.customer_id=orders.customer_id
and orders.order_purchase_timestamp > first_order
and orders.order_purchase_timestamp < date_add(first_order, interval 6 month)
group by a.customer_id)

select 100*(count(distinct a.customer_id)/count( distinct b.customer_id)) as Customer_Retention from a left join b 
on a.customer_id=b.customer_id

"""
cur.execute(query)
data=cur.fetchall()
data
df=pd.DataFrame(data, columns = ["Customer Retention Percentage"])
df


# ## 5. Identify the top 3 customers who spent the most money in each year.

# In[69]:


query = """

select years, customer_id, Order_value,d_rank from (select orders.order_id,
orders.customer_id,
year(orders.order_purchase_timestamp) as years,
round(sum(payments.payment_value),2) as Order_value,
dense_rank() over( partition by year(orders.order_purchase_timestamp) order by sum(payments.payment_value) desc) as d_rank
from orders
join payments on payments.order_id=orders.order_id
group by orders.order_id, orders.customer_id, years order by years, Order_value desc) as a
where d_rank <=3


"""
cur.execute(query)
data=cur.fetchall()
data
df=pd.DataFrame(data, columns = ["Year","Customer_ID","Order Value", "Rank"])
df


# In[ ]:




