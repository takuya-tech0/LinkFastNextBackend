from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import mysql.connector
from mysql.connector import Error
from typing import Dict, Any, List, Optional
from datetime import datetime
import json

class Customer(BaseModel):
    customer_id: str
    customer_name: str
    age: int
    gender: str

class MySQLConnector:
    def __init__(self):
        self.config = {
            'host': 'tech0-db-step4-studentrdb-3.mysql.database.azure.com',
            'user': 'tech0gen7student',
            'password': 'vY7JZNfU',
            'database': 'gen9-practical',
            'client_flags': [mysql.connector.ClientFlag.SSL],
            'ssl_ca': 'DigiCertGlobalRootCA.crt.pem'
        }
        self.connection = None

    def connect(self):
        try:
            if not self.connection or not self.connection.is_connected():
                self.connection = mysql.connector.connect(**self.config)
        except Error as e:
            print(f"Error: {e}")
            raise

    def disconnect(self):
        if self.connection and self.connection.is_connected():
            self.connection.close()

    def execute_query(self, query: str, params: tuple = None) -> Optional[List[Dict]]:
        try:
            self.connect()
            cursor = self.connection.cursor(dictionary=True)
            cursor.execute(query, params)
            
            if query.strip().upper().startswith('SELECT'):
                result = cursor.fetchall()
                return result
            else:
                self.connection.commit()
                return None
                
        except Error as e:
            print(f"Error: {e}")
            raise
        finally:
            if cursor:
                cursor.close()

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db = MySQLConnector()

@app.get("/")
def index():
    return {"message": "FastAPI top page!"}

@app.post("/customers")
def create_customer(customer: Customer):
    query = """
    INSERT INTO customers (customer_id, customer_name, age, gender)
    VALUES (%s, %s, %s, %s)
    """
    values = (customer.customer_id, customer.customer_name, customer.age, customer.gender)
    db.execute_query(query, values)
    
    select_query = "SELECT * FROM customers WHERE customer_id = %s"
    result = db.execute_query(select_query, (customer.customer_id,))
    return result[0] if result else None

@app.get("/customers")
def read_one_customer(customer_id: str = Query(...)):
    query = "SELECT * FROM customers WHERE customer_id = %s"
    result = db.execute_query(query, (customer_id,))
    if not result:
        raise HTTPException(status_code=404, detail="Customer not found")
    return result[0]

@app.get("/allcustomers")
def read_all_customer():
    query = "SELECT * FROM customers"
    result = db.execute_query(query)
    return result if result else []

@app.put("/customers")
def update_customer(customer: Customer):
    query = """
    UPDATE customers 
    SET customer_name = %s, age = %s, gender = %s 
    WHERE customer_id = %s
    """
    values = (customer.customer_name, customer.age, customer.gender, customer.customer_id)
    db.execute_query(query, values)
    
    select_query = "SELECT * FROM customers WHERE customer_id = %s"
    result = db.execute_query(select_query, (customer.customer_id,))
    if not result:
        raise HTTPException(status_code=404, detail="Customer not found")
    return result[0]

@app.delete("/customers")
def delete_customer(customer_id: str = Query(...)):
    query = "DELETE FROM customers WHERE customer_id = %s"
    db.execute_query(query, (customer_id,))
    return {"customer_id": customer_id, "status": "deleted"}
