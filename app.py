#from sqlalchemy import create_engine
from flask.helpers import url_for
import psycopg2
import pandas as pd
from flask import Flask, render_template, request
from werkzeug.utils import redirect
import re

app = Flask(__name__)
#engine = create_engine('postgresql://postgres:pass123@localhost:5432/postgres')
#conn = psycopg2.connect("dbname=postgres user=postgres password=pass123")
conn = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="pass123")
#cur = conn.cursor()
print(conn)

df = pd.read_sql("""SELECT datname FROM pg_database;""", conn)
df.head()
print(df.head())
print(df.columns.values)

#result = df.to_html(classes='table table-striped')
# rj = df.to_json()
#conn.close()

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
            conn = psycopg2.connect(
                    host="localhost",
                    database=query.replace('usa base ', '')[:-1],
                    user="postgres",
                    password="pass123")
        else:
            conn.rollback()
            cur = conn.cursor()
            cur.execute(query)
            conn.commit()
            cur.close()

        #result = df.to_html(classes='table table-striped')
        #handle(result)
    return render_template('index.html', result=result, db_list=db_list)

@app.route("/send_query", methods=['POST'])
def send_query():
    query = request.form.get('query')
    print(query)
    #result = df.to_html(classes='table table-striped')
    result = df.to_html(classes='table table-striped')
    return redirect(url_for('index'))


def handle(result):
    result = df.to_html(classes='table table-striped')
    return result

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

equivalents = {
    'crea base': 'create database',
    'usa': '\c',
    'borra base': 'delete database',
    'crea tabla': 'create table',
    'borra tabla': 'drop table',
    'agrega campo': 'alter table',
    'agrega columna': 'add column',
    'inserta en': 'insert into',
    'valores': 'values',
    'borra': 'delete from',
    'donde': 'where',
    'lista': 'select'
}

'''
    if 'lista' in query:
        query = query.replace('lista', 'select')
        #query = query.replace('de', 'from')
        query = query.replace('[', '', 1)
        query = query.replace(']', ' from', 1)
        #to_move = re.search(r'\[(.*?)\]', query)
        #query = query.replace('['+to_move+'] ', '')
        #to_move = to_move[:0] + '(' + to_move[0:] + ')'
        #query = query.replace('donde', 'donde ' + to_move)
        if 'donde' in query:
            to_move = re.search(r'\[(.*?)\]', query)
            query = query.replace('['+to_move+'] ', '')
            to_move = to_move[:0] + '(' + to_move[0:] + ')'
            query = query.replace('donde', 'donde ' + to_move)

            query = query.replace('donde', 'where')
            if '[y]' in query:
                query = query.replace('[y]', 'and')
            if '[o]' in query:
                query = query.replace('[o]', 'or')
'''