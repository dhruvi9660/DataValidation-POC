import pandas as pd 
import snowflake.connector as sf
from flask import Flask, request

app=Flask(__name__)
user="pocValidation"
password="ValidationPocl"
account="YIZMGGL-RAB12198"
database="DV_POC_DB"
warehouse="CONPUTE_WH"
schema="DB_POC_ETL"
role="ACCOUNTADMIN"

conn=sf.connect(user=user,
                password=password,
                account=account,
                database=database,
                warehouse=warehouse, role=role, schema=schema)

def run_query(conn, query):
    print("Executing the {} query".format(query))
    cursor = conn. cursor ()
    try:
        cursor. execute (query)
        data = cursor.fetchall()
        df = pd.DataFrame(data, columns=[x[0] for x in cursor.description])
        print (df)
    except Exception as e:
        print(e)
        cursor.close()
        return df.to_dict()


@app.route('/')
def hello_world():
    return "Hello World , Welcome to Snowflake API"


app.route('/count', methods=['GET','POST'])
def count():
    query_to_be_executed = "SELECT * FROM DV_POC_ETL.TEST_RESULT_COUNT;"
    return run_query(conn, query_to_be_executed)



@app.route('/duplicate count', methods=['GET','POST'])
def duplicate_count():
    query_to_be_executed = "SELECT * FROM DV_POC_ETL.TEST_RESULT_DUPLICATE;"
    return run_query(conn, query_to_be_executed)


@app.route('/null_ percent', method=['GET','POST'])
def null_percent():
    query_to_be_executed = "SELECT * FROM DV_POC_ETL.TEST_RESULT_NULL_PERCENT;"
    return run_query(conn, query_to_be_executed)

if __name__=='__main__':
    app.run(debug=True)