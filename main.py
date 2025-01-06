from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from contextlib import asynccontextmanager
import aiomysql
import os
from typing import Dict, Any, List, Optional

class Customer(BaseModel):
   customer_id: str
   customer_name: str
   age: int
   gender: str

class MySQLConnector:
   def __init__(self):
       self.config = {
           'host': os.getenv('MYSQL_HOST', 'tech0-db-step4-studentrdb-3.mysql.database.azure.com'),
           'user': os.getenv('MYSQL_USER', 'tech0gen7student'),
           'password': os.getenv('MYSQL_PASSWORD', 'vY7JZNfU'),
           'db': os.getenv('MYSQL_DATABASE', 'gen9-practical'),
           'ssl': {'ca': os.getenv('SSL_CA', '/home/site/certificates/DigiCertGlobalRootCA.crt.pem')}
       }
       self.pool = None

   async def init_pool(self):
       if not self.pool:
           self.pool = await aiomysql.create_pool(
               maxsize=5,
               **self.config
           )

   @asynccontextmanager
   async def get_connection(self):
       if not self.pool:
           await self.init_pool()
       async with self.pool.acquire() as conn:
           async with conn.cursor(aiomysql.DictCursor) as cur:
               yield cur, conn

   async def execute_query(self, query: str, params: tuple = None):
       async with self.get_connection() as (cur, conn):
           await cur.execute(query, params or ())
           if query.strip().upper().startswith('SELECT'):
               result = await cur.fetchall()
               return result
           await conn.commit()
           return None

app = FastAPI()

app.add_middleware(
   CORSMiddleware,
   allow_origins=["*"],
   allow_credentials=True,
   allow_methods=["*"],
   allow_headers=["*"],
)

db = MySQLConnector()

@app.on_event("startup")
async def startup():
   await db.init_pool()

@app.get("/")
async def index():
   return {"message": "FastAPI top page!"}

@app.post("/customers")
async def create_customer(customer: Customer):
   try:
       query = """
       INSERT INTO customers (customer_id, customer_name, age, gender)
       VALUES (%s, %s, %s, %s)
       """
       values = (customer.customer_id, customer.customer_name, customer.age, customer.gender)
       await db.execute_query(query, values)
       
       select_query = "SELECT * FROM customers WHERE customer_id = %s"
       result = await db.execute_query(select_query, (customer.customer_id,))
       return result[0] if result else None
   except Exception as e:
       raise HTTPException(status_code=500, detail=str(e))

@app.get("/customers")
async def read_one_customer(customer_id: str = Query(...)):
   try:
       query = "SELECT * FROM customers WHERE customer_id = %s"
       result = await db.execute_query(query, (customer_id,))
       if not result:
           raise HTTPException(status_code=404, detail="Customer not found")
       return result[0]
   except Exception as e:
       raise HTTPException(status_code=500, detail=str(e))

@app.get("/allcustomers")
async def read_all_customer():
   try:
       query = "SELECT * FROM customers"
       result = await db.execute_query(query)
       return result or []
   except Exception as e:
       raise HTTPException(status_code=500, detail=str(e))

@app.put("/customers")
async def update_customer(customer: Customer):
   try:
       query = """
       UPDATE customers 
       SET customer_name = %s, age = %s, gender = %s 
       WHERE customer_id = %s
       """
       values = (customer.customer_name, customer.age, customer.gender, customer.customer_id)
       await db.execute_query(query, values)
       
       select_query = "SELECT * FROM customers WHERE customer_id = %s"
       result = await db.execute_query(select_query, (customer.customer_id,))
       if not result:
           raise HTTPException(status_code=404, detail="Customer not found")
       return result[0]
   except Exception as e:
       raise HTTPException(status_code=500, detail=str(e))

@app.delete("/customers")
async def delete_customer(customer_id: str = Query(...)):
   try:
       query = "DELETE FROM customers WHERE customer_id = %s"
       await db.execute_query(query, (customer_id,))
       return {"customer_id": customer_id, "status": "deleted"}
   except Exception as e:
       raise HTTPException(status_code=500, detail=str(e))
