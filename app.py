#from sqlalchemy import create_engine
from flask.helpers import url_for
import mysql.connector
import pandas as pd
from flask import Flask, render_template, request
from werkzeug.utils import redirect

app = Flask(__name__)

conn = mysql.connector.connect(
  host="localhost",
  user="root",
  password="spk117",
  database="practica2"
)

print(conn)

databases = pd.read_sql("SELECT schema_name FROM information_schema.schemata WHERE schema_name not in ('information_schema','mysql','performance_schema','sys');", conn).values

@app.context_processor
def utility_processor():
    def get_tables(db):
        return pd.read_sql(f"SHOW TABLES FROM {db[0]};", conn).values

    def get_columns(table, db):
        return pd.read_sql(f"SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '{db[0]}' AND TABLE_NAME = '{table[0]}';", conn).values
    
    return dict(get_tables=get_tables, get_columns=get_columns)

db_list = ['postgres', 'db1', 'db2']
result = ""

@app.route("/", methods=['POST', 'GET'])
def index():
    if request.method == 'POST':
        query = request.form.get('query')
        query = reserved_words(query)
        global result 
        global conn

        if 'select' in query:
            conn.rollback()
            df = pd.read_sql(query, conn)
            result = df.to_html(classes='table table-striped')
        elif 'usa base' in query:
            conn.close()
            conn = mysql.connector.connect(
                    host="localhost",
                    user="root",
                    password="spk117",
                    database=query.replace('usa base ', '')[:-1])
        else:
            conn.rollback()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            cur.close()

    return render_template('index.html', result=result, db_list=databases)


def reserved_words(query: str):
    if 'lista' in query:
        query = query.replace('lista', 'select')
        query = query.replace('[', '', 1)
        query = query.replace(']', ' from', 1)
        if 'donde' in query:
            query = query.replace('donde', 'where')
            if '[y]' in query:
                query = query.replace('[y]', 'and')
            if '[o]' in query:
                query = query.replace('[o]', 'or')
    elif 'crea base' in query:
        query = query.replace('crea base', 'create database')
    elif 'borra base' in query:
        query = query.replace('borra base', 'drop database')
    elif 'crea tabla' in query:
        query = query.replace('crea tabla', 'create table')
    elif 'borra tabla' in query:
        query = query.replace('borra tabla', 'drop table')
    elif 'agrega campo' in query:
        occurrences = 0
        query = query.replace('agrega campo', 'alter table')
        for i in range(len(query)):
            if query[i] == ' ':
                occurrences += 1
            if occurrences == 3:
                query = query[:i] + ' add column' + query[i:]
                break
        if ',' in query:
            query = query.replace(',', ', add column')
    elif 'borra campo' in query:
        occurrences = 0
        query = query.replace('borra campo', 'alter table')
        for i in range(len(query)):
            if query[i] == ' ':
                occurrences += 1
            if occurrences == 3:
                query = query[:i] + ' drop column' + query[i:]
                break
        if ',' in query:
            query = query.replace(',', ', drop column')
    elif 'inserta en' in query:
        query = query.replace('inserta en', 'insert into')
        query = query.replace('valores', 'values')
    elif 'borra' in query:
        query = query.replace('borra', 'delete from')
        query = query.replace('donde', 'where')

    return query
