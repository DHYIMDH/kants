#!/usr/bin/env python
# coding: utf-8

# ### Preaquisition
# - 자신의 아나콘다 가상 환경에 psycopg2 설치 필요

# In[2]:


from psycopg2 import connect


# In[6]:


if connect(database="postgres", user='postgres', password='kants123!', host='34.64.47.191'):
    print("connected")
else:
    print("not connected")


# In[26]:


def TestDB():
    with connect(database="postgres", user='postgres', password='kants123!', host='34.64.47.191') as conn:
        
        with conn.cursor() as cur:
            
            DB_NAME = "testdb"
            
            # if testdb exists then delete it
            cur.execute("DROP TABLE IF EXISTS " + str(DB_NAME) + ";")
            
            # create testdb
            cur.execute("CREATE TABLE " + str(DB_NAME) + "(            date VARCHAR(20) not null,            ticker_supplier VARCHAR(50) not null,            ticker_customer VARCHAR(50) not null,            r_strength NUMERIC(20,2),            PRIMARY KEY (date, ticker_supplier, ticker_customer)            );")
            
            
            # make sample data
            sample = ["20221102", "005930", "APPL", "25.4"]
            
            # insert sample data
            cur.execute(
            "INSERT INTO "+str(DB_NAME)+"(date, ticker_supplier, ticker_customer, r_strength) VALUES (%s, %s, %s, %s)",
                (sample[0], sample[1], sample[2], sample[3]))
            print(sample[0], sample[1], sample[2], sample[3] + " is inserted into testdb.")
            conn.commit()
            
            # print data in db
            cur.execute("SELECT * FROM "+ str(DB_NAME) +";")
            rows = cur.fetchall()
            
            for i in rows:
                print(i)
            
            # delete testdb table
            cur.execute("DROP TABLE IF EXISTS " + str(DB_NAME) + ";")
            
            # close the cursor to avoid memory leaks
            cur.close


# In[27]:


if __name__ == "__main__":
    TestDB()

