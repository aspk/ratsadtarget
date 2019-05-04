from app import app
from flask import render_template
import psycopg2
import random
import time


@app.route('/')
@app.route('/index')
def index():
    def select_table():
        conn_string = "host='host' port='port' dbname='dbname' user='user' password='password'"

        conn = psycopg2.connect(conn_string)

        cursor = conn.cursor()

        cursor.execute(
            "SELECT * FROM (SELECT * FROM testtable9 ORDER BY timestamp DESC LIMIT 5) as a ORDER BY a.numcomments DESC")

        records = cursor.fetchall()
        # print(records)
        return records
    while True:
        try:
            records = select_table()[:10]
            break
        except:
            time.sleep(random.random())
    user = {'username': 'username'}
    posts = []
    for x in records:
        xtemp = {}
        xtemp['post'] = x[1]
        xtemp['num'] = x[2]
        posts.append(xtemp)

    return render_template('index2.html', title='Home', user=user, posts=posts)
