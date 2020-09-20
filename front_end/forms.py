from flask_wtf import FlaskForm
from wtforms import StringField, SubmitField
from wtforms.validators import DataRequired

from flask import Flask, render_template

class submitQuery(FlaskForm):
    query = StringField('Query', validators=[DataRequired()])
    submit = SubmitField('Execute')
