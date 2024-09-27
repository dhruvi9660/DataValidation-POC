import pandas as pd 
import snowflake.connector as sf
from flask import Flask, request
from flask import *
from flask import *

app=Flask(__name__)
user="pocValidation"
password="ValidationPoc1"
password="ValidationPoc1"
account="YIZMGGL-RAB12198"
database="DV_POC_DB"
warehouse="COMPUTE_WH"
warehouse="COMPUTE_WH"
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
    return df


@app.route('/')
def hello_world():
    return render_template('index.html')


@app.route('/count_check', methods=['GET','POST'])
def count_check():
    query_to_be_executed = "SELECT * FROM DV_POC_ETL.TEST_RESULT_COUNT;"
    df=run_query(conn, query_to_be_executed)
    print(df.columns.values)
    col_names = ['SOURCE_TYPE', 'SOURCE_SCHEMA_NAME', 'SOURCE_OBJECT_NAME' ,'TARGET_TYPE',
    'TARGET_SCHEMA_NAME', 'TARGET_OBJECT_NAME', 
    'SOURCE_COUNT', 'TARGET_COUNT' ,'SOURCE_SQL', 'TARGET_SQL', 'EXECUTION_DT', 'TEST_RESULT']
    records = df.to_dict(orient='records')
    return render_template('count_check.html', col_names = col_names, records=records,title='Count Check')



@app.route('/duplicate_count', methods=['GET','POST'])
def duplicate_count():
    query_to_be_executed = "SELECT * FROM DV_POC_ETL.TEST_RESULT_DUPLICATE;"
    df =  run_query(conn, query_to_be_executed)
    print(df.columns.values)
    col_names = ['SOURCE TYPE' ,'SOURCE SCHEMA NAME' ,'SOURCE OBJECT NAME' ,'TARGET TYPE',
 'TARGET SCHEMA NAME' ,'TARGET OBJECT NAME'  ,'DUPLICATE COUNT',
 'TARGET PRIMARY KEY', 'DUPLICATE SQL' ,'EXECUTION DT','TEST RESULT']
    records = df.to_dict(orient='records')
    return render_template('duplicate_check.html', col_names = col_names, records=records, title='Duplicate Check')


@app.route('/null_percent', methods=['GET','POST'])
def null_percent():
    query_to_be_executed = "SELECT * FROM DV_POC_ETL.TEST_RESULT_NULL_PERCENT;"
    df=run_query(conn, query_to_be_executed)
    col_names = ['SOURCE TYPE', 'SOURCE SCHEMA NAME', 'SOURCE OBJECT NAME', 'TARGET TYPE',
    'TARGET SCHEMA NAME' ,'TARGET OBJECT NAME' ,'COLUMN NAME',
     'NULL COUNT', 'NOT NULL COUNT' ,'TOTAL COUNT' ,'NULL PERCENT', 'EXECUTION DT','TEST RESULT']
    records = df.to_dict(orient='records')
    return render_template('null_percent.html', col_names = col_names, records=records,title='Null Percent Check')

if __name__=='__main__':
    app.run(debug=True)
