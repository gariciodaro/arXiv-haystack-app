from flask import render_template, redirect, url_for
from flask import flash, get_flashed_messages
import pandas as pd
from app import app
import forms
import psycopg2
from flask import Markup

import configparser

config = configparser.ConfigParser()

# AWS credentials
config.read('/home/gari/.aws/credentials')
KEY      = config.get('credentials','KEY')
SECRET   = config.get('credentials','SECRET')


DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")
DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")


@app.route('/')
@app.route('/index' , methods=['GET','POST'])
def index():
    form = forms.submitQuery()
    print(form.validate_on_submit())
    if form.validate_on_submit():
        conn = psycopg2.connect(
            dbname=DWH_DB, 
            host='', 
            port= DWH_PORT,
            user= DWH_DB_USER, 
            password= DWH_DB_PASSWORD)
        cur = conn.cursor()
        print('query')
        print(form.query.data)
        cur.execute(form.query.data)
        results = cur.fetchall()
        cur.close()
        df = pd.DataFrame(results)
        value = Markup(df.to_html(header="true", table_id="table"))
        #results = cur.fetchone()
        return render_template('index.html',form=form,result=value)

    return render_template('index.html',form=form)

@app.route('/db_info')
def db_info():
    return render_template('db_info.html')

@app.route('/spark_pro')
def spark_pro():
    return render_template('spark_pro.html')