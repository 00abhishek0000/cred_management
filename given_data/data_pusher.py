from celery import Celery
import pandas as pd
import mysql.connector
from mysql.connector import Error

# Initialize Celery app
app = Celery('data_pusher', backend='rpc://', broker='pyamqp://guest:guest@localhost//')

# Define MySQL database connection
DATABASE_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'abhishek1397',
    'database': 'alemeno'
}

# Connect to MySQL database
try:
    connection = mysql.connector.connect(**DATABASE_CONFIG)
    if connection.is_connected():
        print('Connected to MySQL database')

except Error as e:
    print(f'Error while connecting to MySQL: {e}')

# Celery task to ingest data
@app.task
def ingest_data():
    try:
        # Read customer data from Excel file
        customer_df = pd.read_excel('customer_data.xlsx')
        loan_df = pd.read_excel('loan_data.xlsx')

        # Ingest customer data
        cursor = connection.cursor()
        cursor.execute("""
                CREATE TABLE IF NOT EXISTS customers (
                id INT AUTO_INCREMENT PRIMARY KEY,
                customer_id INT ,
                first_name VARCHAR(255),
                last_name VARCHAR(255),
                phone_number VARCHAR(255),
                monthly_salary FLOAT,
                approved_limit FLOAT,
                current_debt FLOAT
            )
        """)

        cursor.execute("""
                CREATE TABLE IF NOT EXISTS loans (
                id INT AUTO_INCREMENT PRIMARY KEY,
                loan_id INT ,
                customer_id INT,
                loan_amount FLOAT,
                tenure INT,
                interest_rate FLOAT,
                monthly_payment FLOAT,
                emis_paid_on_time INT,
                date_of_approval DATE,
                end_date DATE
            )
        """)

        for index, row in customer_df.iterrows():
            sql = "INSERT INTO customers (customer_id,first_name, last_name, phone_number, monthly_salary, approved_limit, current_debt) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, (row['Customer ID'], row['First Name'], row['Last Name'], row['Phone Number'], row['Monthly Salary'], row['Approved Limit'], row['Current Debt']))
        connection.commit()
        print(f"{cursor.rowcount} records inserted into customers table")

        for index, row in loan_df.iterrows():
            sql = "INSERT INTO loans (customer_id, loan_id, loan_amount, tenure, interest_rate, monthly_payment, emis_paid_on_time, date_of_approval, end_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            cursor.execute(sql, (row['Customer ID'], row['Loan ID'], row['Loan Amount'], row['Tenure'], row['Interest Rate'], row['Monthly payment'], row['EMIs paid on Time'], row['Date of Approval'], row['End Date']))
        connection.commit()

    except FileNotFoundError as e:
        print(f"File not found: {e}")

    except ImportError as e:
        print(f"Error importing pandas: {e}")

    except Error as e:
        print(f'Error while ingesting data into MySQL: {e}')

    finally:
        if 'cursor' in locals() and cursor is not None:
            cursor.close()
        if 'connection' in locals() and connection.is_connected():
            connection.close()
            print('MySQL connection closed')

if __name__ == '__main__':
    # Trigger Celery task
    ingest_data.delay()
