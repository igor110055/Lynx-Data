from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask import render_template
from flask_bootstrap import Bootstrap5
from flask_wtf import FlaskForm
from wtforms import StringField, IntegerField
from wtforms.validators import InputRequired
import binance_order


app = Flask(__name__)
app.config['SECRET_KEY'] = 'secretkey'
bootstrap = Bootstrap5(app)

class BinanceForm(FlaskForm):
    pair1 = StringField('Pair', validators=[InputRequired('Enter the Trading Pair')])
    pair2 = StringField('Pair', validators=[InputRequired('Enter the Trading Pair')])
    pair3 = StringField('Pair', validators=[InputRequired('Enter the Trading Pair')])
    amount = IntegerField('Amount', validators=[InputRequired('Enter the Quantity of the first pair')])


db_name = 'databases/Arby.db'

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + db_name

app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = True

db = SQLAlchemy(app)


class Arby(db.Model):
    __tablename__ = 'arby'
    index = db.Column(db.Integer)
    time = db.Column(db.String)
    pair = db.Column(db.String)
    profit = db.Column(db.Float)
    bid = db.Column(db.Float)
    ask = db.Column(db.Float)
    volume = db.Column(db.String, primary_key=True)
    latency = db.Column(db.String)
    purchased = db.Column(db.Float)


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/data')
def arbys():
    arby = Arby.query.all()
    form = BinanceForm()

    if form.validate_on_submit():
        binance_order.place_order(form.pair1.data, form.pair2.data, form.pair3.data, form.amount.data)
        return render_template('list.html', arby=arby, form=form)



if __name__ == '__main__':
    app.run(debug=True)
