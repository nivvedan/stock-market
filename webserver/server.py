#!/usr/bin/env python2.7

"""
Columbia W4111 Intro to databases
Example webserver

To run locally

    python server.py

Go to http://localhost:8111 in your browser


A debugger such as "pdb" may be helpful for debugging.
Read about it online.

eugene wu 2015
"""

import os
import marketorders

from datetime import datetime
from sqlalchemy import *
from sqlalchemy.pool import NullPool
from flask import Flask, request, render_template, g, redirect, Response

tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
app = Flask(__name__, template_folder=tmpl_dir)


#
# The following uses the sqlite3 database test.db -- you can use this for debugging purposes
# However for the project you will need to connect to your Part 2 database in order to use the
# data
#
# XXX: The URI should be in the format of: 
#
#     postgresql://USER:PASSWORD@w4111db1.cloudapp.net:5432/proj1part2
#
# For example, if you had username ewu2493, password foobar, then the following line would be:
#
#     DATABASEURI = "postgresql://ewu2493:foobar@w4111db1.cloudapp.net:5432/proj1part2"
#
password = input("Password for ns2984: ")
DATABASEURI = "postgresql://ns2984:" + str(password) + "@w4111db1.cloudapp.net:5432/proj1part2"


#
# This line creates a database engine that knows how to connect to the URI above
#
engine = create_engine(DATABASEURI)


#
# START SQLITE SETUP CODE
#
# after these statements run, you should see a file test.db in your webserver/ directory
# this is a sqlite database that you can query like psql typing in the shell command line:
# 
#     sqlite3 test.db
#
# The following sqlite3 commands may be useful:
# 
#     .tables               -- will list the tables in the database
#     .schema <tablename>   -- print CREATE TABLE statement for table
# 
# The setup code should be deleted once you switch to using the Part 2 postgresql database
#
# engine.execute("""DROP TABLE IF EXISTS test;""")
# engine.execute("""CREATE TABLE IF NOT EXISTS test (
#   id serial,
#   name text
# );""")
# engine.execute("""INSERT INTO test(name) VALUES ('grace hopper'), ('alan turing'), ('ada lovelace');""")
#
# END SQLITE SETUP CODE
#



@app.before_request
def before_request():
  """
  This function is run at the beginning of every web request 
  (every time you enter an address in the web browser).
  We use it to setup a database connection that can be used throughout the request

  The variable g is globally accessible
  """
  try:
    g.conn = engine.connect()
  except:
    print("uh oh, problem connecting to database")
    import traceback; traceback.print_exc()
    g.conn = None

@app.teardown_request
def teardown_request(exception):
  """
  At the end of the web request, this makes sure to close the database connection.
  If you don't the database could run out of memory!
  """
  try:
    g.conn.close()
  except Exception as e:
    pass


#
# @app.route is a decorator around index() that means:
#   run index() whenever the user tries to access the "/" path using a POST or GET request
#
# If you wanted the user to go to e.g., localhost:8111/foobar/ with POST or GET then you could use
#
#       @app.route("/foobar/", methods=["POST", "GET"])
#
# PROTIP: (the trailing / in the path is important)
# 
# see for routing: http://flask.pocoo.org/docs/0.10/quickstart/#routing
# see for decorators: http://simeonfranklin.com/blog/2012/jul/1/python-decorators-in-12-steps/
# 
@app.route('/', methods=["POST", "GET"])
def index():
  """
  request is a special object that Flask provides to access web request information:

  request.method:   "GET" or "POST"
  request.form:     if the browser submitted a form, this contains the data in the form
  request.args:     dictionary of URL arguments e.g., {a:1, b:2} for http://localhost?a=1&b=2

  See its API: http://flask.pocoo.org/docs/0.10/api/#incoming-request-data
  """

  # DEBUG: this is debugging code to see what request looks like
  # print(request.args)


  #
  # example of a database query
  #
  cursor = g.conn.execute("SELECT pid, name FROM portfolio")
  pids = []
  names = []
  for result in cursor:
    names.append(result['name'].strip())  # can also be accessed using result[0]
    pids.append(result['pid'])  # can also be accessed using result[0]
  cursor.close()


  #
  # Flask uses Jinja templates, which is an extension to HTML where you can
  # pass data to a template and dynamically generate HTML based on the data
  # (you can think of it as simple PHP)
  # documentation: https://realpython.com/blog/python/primer-on-jinja-templating/
  #
  # You can see an example template in templates/index.html
  #
  # context are the variables that are passed to the template.
  # for example, "data" key in the context variable defined below will be 
  # accessible as a variable in index.html:
  #
  #     # will print: [u'grace hopper', u'alan turing', u'ada lovelace']
  #     <div>{{data}}</div>
  #     
  #     # creates a <div> tag for each element in data
  #     # will print: 
  #     #
  #     #   <div>grace hopper</div>
  #     #   <div>alan turing</div>
  #     #   <div>ada lovelace</div>
  #     #
  #     {% for n in data %}
  #     <div>{{n}}</div>
  #     {% endfor %}
  #
  context = dict(names=names, pids=pids)


  #
  # render_template looks in the templates/ folder for files.
  # for example, the below file reads template/index.html
  #
  return render_template("index.html", **context)

#
# This is an example of a different path.  You can see it at
# 
#     localhost:8111/another/
#
# notice that the function name is another() rather than index()
# the functions for each app.route needs to have different names
#
# @app.route('/another/', methods=["POST", "GET"])
# def another():
#   return render_template("anotherfile.html")

@app.route('/portfolio/<int:pid>/', methods=['GET', 'POST'])
def show_portfolio(pid):
  if not isinstance(pid, int):
    return render_template('404.html'), 404

  cursor = g.conn.execute("SELECT pid, name FROM portfolio WHERE pid = %s;", pid)
  if cursor.rowcount == 0:
    return render_template('404.html'), 404
  for result in cursor:
    portfolio = {'pid': result['pid'], 'name': result['name'].strip()}

  errors = []

  if request.method != "POST":
    return display_stocks(pid, portfolio, errors)

  username = request.form['username'].strip()
  password = request.form['password']
  cursor = g.conn.execute("SELECT username FROM Trader WHERE username = %s AND password = %s;", username, password)
  if cursor.rowcount == 0:
    errors.append("Username or password incorrect.")
    return display_stocks(pid, portfolio, errors)

  cursor = g.conn.execute("SELECT portfolio FROM Trader_Manages WHERE trader = %s AND portfolio = %s;", username, pid)
  if cursor.rowcount == 0:
    errors.append("Trader not authorized for this Portfolio.")
    return display_stocks(pid, portfolio, errors)
  
  ticker = request.form['ticker'].strip()
  cursor = g.conn.execute("SELECT ticker FROM Stock WHERE ticker = %s;", ticker)
  if cursor.rowcount == 0:
    errors.append("No such Stock Ticker exists in the database.")
    return display_stocks(pid, portfolio, errors)

  quantity = request.form['quantity'].strip()
  try:
    quantity = int(quantity)
  except:
    errors.append("Quantity should be an integer.")
    return display_stocks(pid, portfolio, errors)
  if quantity <= 0:
    errors.append("Quantity should be a positive integer.")
    return display_stocks(pid, portfolio, errors)

  cprice = request.form['cprice'].strip()
  if not cprice and request.form['price'] == 'custom':
    errors.append("Custom price selected but not specified.")
    return display_stocks(pid, portfolio, errors)
  
  if request.form['price'] == 'custom':
    try:
      cprice = float(cprice)
    except:
      errors.append("Custom price should be a number.")
      return display_stocks(pid, portfolio, errors)
    if cprice < 0:
      errors.append("Custom price cannot be negative.")
      return display_stocks(pid, portfolio, errors)
  else:
    cprice = None

  try:
    if not check_assets(pid, request.form['order'], ticker,
                        request.form['price'] == 'market', cprice, quantity):
      errors.append("You don't have sufficient funds / stocks to execute " + \
                    "this order.")
      return display_stocks(pid, portfolio, errors)

    cursor = g.conn.execute("INSERT INTO Trade_Order(type, stock, market, " + \
                            "unit_price, quantity, trader, portfolio, " + \
                            "timestamp) VALUES(%s, %s, %s, %s, %s, %s, %s, " + \
                            "%s);", request.form['order'], ticker,
                            request.form['price'] == 'market', cprice, quantity,
                            username, pid, datetime.now())
    process_orders(ticker)
  except:
    errors.append("Invalid Order.")
    return display_stocks(pid, portfolio, errors) 

  return display_stocks(pid, portfolio, errors)

def check_assets(pid, order, ticker, market, cprice, quantity):
  if order == "BUY":
    if market:
      cursor = g.conn.execute("SELECT market_price FROM Stock WHERE ticker" + \
                              " = %s", ticker)
      for result in cursor:
        mp = float(result['market_price'][1:])
        fundsneeded = mp * quantity
    else:
      fundsneeded = cprice * quantity
    cursor = g.conn.execute("SELECT cash FROM Portfolio WHERE pid = %s", pid)
    for result in cursor:
      cashavailable = float(result['cash'].replace(',', "")[1:])
      if cashavailable < fundsneeded:
        return False

  elif order == "SELL":
    cursor = g.conn.execute("SELECT quantity FROM Portfolio_Stock WHERE " + \
                            "portfolio = %s AND stock = %s", pid, ticker)
    if cursor.rowcount == 0:
      return False
    for result in cursor:
      qtyavailable = int(result['quantity'])
      if qtyavailable < quantity:
        return False

  return True   


def display_stocks(pid, portfolio, errors):
  cursor = g.conn.execute("SELECT stock, company_name, quantity, market_price FROM StockHoldings " + \
                          "WHERE portfolio = %s;", pid)
  stocks = []
  for result in cursor:
    stock = {}
    stock['ticker'] = result['stock'].strip()
    stock['company_name'] = result['company_name']
    stock['quantity'] = result['quantity']
    stock['market_price'] = result['market_price']
    stocks.append(stock)
  
  return render_template("portfolio.html", **dict(stocks=stocks, portfolio=portfolio, errors=errors))

@app.route('/stock/<ticker>/')
def show_stock(ticker):
  if not isinstance(ticker, str) and not isinstance(ticker, unicode):
    return render_template('404.html'), 404

  cursor = g.conn.execute("SELECT ticker, company_name FROM Stock WHERE ticker = %s;", ticker)
  if cursor.rowcount == 0:
    return render_template('404.html'), 404
  for result in cursor:
    stock = {'ticker': result['ticker'].strip(), 'company_name': result['company_name'].strip()}

  cursor = g.conn.execute("SELECT type, unit_price, quantity, portfolio FROM Trade_Order " + \
                          "WHERE stock = %s AND market = False;", ticker)
  buyorders = []
  sellorders = []
  for result in cursor:
    order = {}
    order['unit_price'] = result['unit_price']
    order['quantity'] = result['quantity']
    order['pid'] = result['portfolio']
    if result['type'].strip().lower() == 'buy':
      buyorders.append(order)
    else:
      sellorders.append(order)

  buyorders.sort(key=lambda order: order['unit_price'], reverse=True)
  sellorders.sort(key=lambda order: order['unit_price'])
  
  return render_template("stock.html", **dict(stock=stock, buyorders=buyorders,
                                              sellorders=sellorders))

def process_orders(ticker):
  # market orders
  cursor_sell_mt = g.conn.execute("SELECT id, portfolio, quantity, unit_price FROM Trade_Order WHERE stock = %s AND type = SELL AND market = TRUE ORDER BY price ASC;", ticker)
  cursor_buy_mt = g.conn.execute("SELECT id, portfolio, quantity, unit_price FROM Trade_Order WHERE stock = %s AND type = BUY AND market = TRUE ORDER BY price DESC;", ticker)
  # normal orders
  cursor_sell = g.conn.execute("SELECT id, portfolio, quantity, unit_price FROM Trade_Order WHERE stock = %s AND type = SELL ORDER BY price ASC;", ticker)
  cursor_buy = g.conn.execute("SELECT id, portfolio, quantity, unit_price FROM Trade_Order WHERE stock = %s AND type = BUY ORDER BY price DESC;", ticker)
  
  if cursor_sell_mt.rowcount != 0 and cursor_buy.rowcount != 0:
    exec_sell_mt(ticker, cursor_sell_mt, cursor_buy)
  elif cursor_buy_mt.rowcount != 0 and cursor_sell.rowcount != 0:
    exec_buy_mt(ticker, cursor_buy_mt, cursor_sell)
  
  if cursor_sell.rowcount == 0 or cursor_buy.rowcount == 0:
    return True

  for buy_order in cursor_buy:
    for sell_order in cursor_sell:
      if buy_order['unit_price'] >= sell_order['unit_price']:
        qty_diff = buy_order['quantity'] - sell_order['quantity'] 
        if qty_diff == 0:
          g.conn.execute("DELETE FROM Trade_Order WHERE pid = %s and quantity = %s and unit_price = %s and type = SELL;",
                            sell_order['pid'], sell_order['quantity'], sell_order['unit_price'])
          g.conn.execute("DELETE FROM Trade_Order WHERE pid = %s and quantity = %s and unit_price = %s and type = BUY;",
                            buy_order['pid'], buy_order['quantity'], buy_order['unit_price'])
        elif qty_diff > 0:  #buy order quantity is more
          g.conn.execute("DELETE FROM Trade_Order WHERE pid = %s and quantity = %s and unit_price = %s and type = SELL;", 
                            sell_order['pid'], sell_order['quantity'], sell_order['unit_price'])
          g.conn.execute("UPDATE FROM Trade_Order SET quantity = %s - %s WHERE pid = %s and quantity = %s and unit_price = %s and type = BUY;", buy_order['quantity'], qty_diff, buy_order['pid'], buy_order['quantity'], buy_order['unit_price'])
        else:  #buy order quantity is less
          g.conn.execute("UPDATE FROM Trade_Order SET quantity = %s + %s WHERE pid = %s and quantity = %s and unit_price = %s and type = SELL;", sell_order['quantity'], qty_diff, sell_order['pid'], sell_order['quantity'], sell_order['unit_price'])     
          g.conn.execute("DELETE FROM Trade_Order WHERE pid = %s and quantity = %s and unit_price = %s and type = BUY;", 
                            buy_order['pid'], buy_order['quantity'], buy_order['unit_price'])

          ## TODO update portfolio, transaction

if __name__ == "__main__":
  import click

  @click.command()
  @click.option('--debug', is_flag=True)
  @click.option('--threaded', is_flag=True)
  @click.argument('HOST', default='0.0.0.0')
  @click.argument('PORT', default=8111, type=int)
  def run(debug, threaded, host, port):
    """
    This function handles command line parameters.
    Run the server using

        python server.py

    Show the help text using

        python server.py --help

    """

    HOST, PORT = host, port
    print("running on %s:%d" % (HOST, PORT))
    app.run(host=HOST, port=PORT, debug=debug, threaded=threaded)


  run()
