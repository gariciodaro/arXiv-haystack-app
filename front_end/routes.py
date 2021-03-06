from flask import render_template, redirect, url_for
from flask import request
from flask import flash, get_flashed_messages
import pandas as pd
from app import app
import forms
import psycopg2
from flask import Markup
import matplotlib as mpl
import matplotlib.pyplot as plt
from haystack.document_store.elasticsearch import ElasticsearchDocumentStore
import boto3
import os
from haystack import Finder
from haystack.reader.farm import FARMReader
from haystack.reader.transformers import TransformersReader
from haystack.utils import print_answers

document_store = ElasticsearchDocumentStore(host="localhost", 
                                            username="", 
                                            password="", 
                                            index="document")

reader = FARMReader(model_name_or_path="distilbert-base-uncased-distilled-squad", use_gpu=False)

from haystack.retriever.sparse import ElasticsearchRetriever
retriever = ElasticsearchRetriever(document_store=document_store)
finder = Finder(reader, retriever)

mpl.style.use(['ggplot'])

import configparser

config = configparser.ConfigParser()

# AWS credentials
try:
    config.read('/home/gari/.aws/credentials')
    KEY      = config.get('credentials','KEY')
except:
    config.read('/home/ubuntu/.aws/credentials')
    KEY      = config.get('credentials','KEY')

SECRET   = config.get('credentials','SECRET')


DWH_CLUSTER_IDENTIFIER = config.get("DWH","DWH_CLUSTER_IDENTIFIER")
DWH_DB                 = config.get("DWH","DWH_DB")
DWH_DB_USER            = config.get("DWH","DWH_DB_USER")
DWH_DB_PASSWORD        = config.get("DWH","DWH_DB_PASSWORD")
DWH_PORT               = config.get("DWH","DWH_PORT")
DWH_IAM_ROLE_NAME      = config.get("DWH", "DWH_IAM_ROLE_NAME")
DWH_HOST               = config.get("DWH", "DWH_HOST")

S3_BUCKED = config.get("APP", "S3_BUCKED")

import re
REPLACE_BY_SPACE_RE = re.compile('[/(){}\[\]\|@,;]')
BAD_SYMBOLS_RE = re.compile('[^0-9a-z #+_]')
def text_prepare(text):
    """
        text: a string
        return: standarized initial string
    """
    try:
        text = text.lower() 
        # replace by space
        text = REPLACE_BY_SPACE_RE.sub(" ",text) 
        # remove bad symbols
        text = BAD_SYMBOLS_RE.sub(" ",text)
        # remove redudant blank spaces
        text = re.sub(r'\s+'," ",text)
    except:
        pass
    return text


@app.route('/')
@app.route('/index' , methods=['GET','POST'])
def index():
    form = forms.submitQuery()
    print(form.validate_on_submit())
    if form.validate_on_submit():
        conn = psycopg2.connect(
            dbname=DWH_DB, 
            host= DWH_HOST, 
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
        value = Markup(df.to_html(header="true", table_id="table", classes='table table-dark'))
        #results = cur.fetchone()
        return render_template('index.html',form=form,result=value)

    return render_template('index.html',form=form)

@app.route('/db_info')
def db_info():
    return render_template('db_info.html')

@app.route('/spark_pro')
def spark_pro():
    return render_template('spark_pro.html')

@app.route('/categories')
def categories():
    return render_template('categories.html')

@app.route('/eda' , methods=['GET','POST'])
def eda():
    print(request.form.get('form1'))
    if request.method == 'POST':
        conn = psycopg2.connect(
            dbname=DWH_DB, 
            host= DWH_HOST, 
            port= DWH_PORT,
            user= DWH_DB_USER, 
            password= DWH_DB_PASSWORD)
        cur = conn.cursor()
        if request.form.get('form1')=='over_years':
            cur.execute('select year,count (distinct id) from versions group by year')
            results = cur.fetchall()
            df = pd.DataFrame(results,
                    columns=('year','total')).astype({'year': 'int32'}).\
                    sort_values(by='year').set_index('year')
            df.plot(kind='bar',figsize=(20, 10))
            plt.title('Paper production over the years')
            plt.ylabel('Total Papers')
            plt.xlabel('Years')
            #plt.show()
            plt.savefig('./static/paper_prod.png')
            cur.close()
            return render_template('eda.html',im1=True)
        if request.form.get('form1')=='predominan':
            cur.execute("""
                select year,category, count(a.id) as count_id from 
                (select id, category from categories) a 
                left join 
                (select id, max(year) as year from versions group by id) b
                on a.id=b.id
                where year>2000
                group by year,category
                """)
            results = cur.fetchall()
            df2 = pd.DataFrame(results,
                  columns=('year',
                        'category',
                        'total')).astype({'year': 'int32'},{'total': 'int32'}).\
                  sort_values(by='year').set_index('year')
            macro_cat=df2['category'].str.split(' ',
                        n=1,expand=True).rename(columns={0:'macro_category'})
            fig=plt.figure(figsize=(17,9))
            ax1=fig.add_subplot(2,2,1)
            ax2=fig.add_subplot(2,2,2)
            ax3=fig.add_subplot(2,2,3)
            ax4=fig.add_subplot(2,2,4)
            fig.suptitle("Frequency of paper by macro-category", fontsize=14)
            for year,ax in zip([2005,2010,2015,2020],[ax1,ax2,ax3,ax4]):
                df_temp=macro_cat.loc[macro_cat.index==year]
                #print(len(df_temp))
                df_temp["macro_category"].value_counts().plot(kind="barh",ax=ax)
                ax.set_xlabel("paper count "+ str(year))
            plt.savefig('./static/paper_topics.png')
            cur.close()
            return render_template('eda.html',im2=True)
    return render_template('eda.html')

@app.route('/qanda_pre' , methods=['GET','POST'])
def qanda_pre():
    if request.method == 'POST':
        conn = psycopg2.connect(
            dbname=DWH_DB, 
            host= DWH_HOST, 
            port= DWH_PORT,
            user= DWH_DB_USER, 
            password= DWH_DB_PASSWORD)
        cur = conn.cursor()
        if request.form.get('form2')=='index_docs':
                cur.execute(request.form.get('query'))
                results = cur.fetchall()
                try:
                    if len(results[0])==1:
                        old=""
                        for each in results:
                            new=each[0]
                            if old=="":
                                old="'"+new+"'"
                            else:
                                old=old+","+"'"+new+"'"
                        cur.execute("""
                            select a.title, b.abstract, a.id from
                             (select id,title from titles where id in ({}) ) a 
                             left join  
                             (select id,abstract from abstracts) b
                             on a.id=b.id  """.format(old))
                        results = cur.fetchall()
                        for result in results:
                            load_dict={}
                            load_dict['name']=result[0]+'|'+result[2]
                            load_dict['text']=result[1]
                            print(load_dict['name'])
                            document_store.write_documents([load_dict])
                        flash('Abstracts were indexed!')
                    else:
                        flash('Not a good query')
                    cur.close()
                    return render_template('qanda_pre.html')
                except:
                    flash('Not a good query')
                    cur.close()
                    return render_template('qanda_pre.html')
    else:
        return render_template('qanda_pre.html')

@app.route('/qanda' , methods=['GET','POST'])
def qanda():
    if request.method == 'POST':
        prediction = finder.get_answers(question=request.form.get('query'), top_k_retriever=5, top_k_reader=5)
        return render_template('qanda.html',prediction=prediction['answers'])
    else:
        return render_template('qanda.html')