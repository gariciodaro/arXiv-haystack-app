from flask import Flask, render_template
#from flask_sqlalchemy import SQLAlchemy

app= Flask(__name__)
app.config['SECRET_KEY']= 'tanenboaun'
#app.config['SQLALCHEMY_DATABASE_URI']= 'sqlite:///data.bd'

#db= SQLAlchemy(app)

# routes should be imported after app
# it wont work if you do it otherwise
from routes import *

if __name__ =='__main__':
    app.run(debug=True)