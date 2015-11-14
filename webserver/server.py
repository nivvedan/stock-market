#!/usr/bin/env python2.7

"""
Columbia W4111 Intro to databases
Stock Market webserver

To run locally

    python server.py

Go to http://localhost:8111 in your browser


A debugger such as "pdb" may be helpful for debugging.
Read about it online.

Kunal Mahajan
Nivvedan Senthamil Selvan
"""

import os

from datetime import datetime
from sqlalchemy import *
from sqlalchemy.pool import NullPool
from flask import Flask, request, render_template, g, redirect, Response

tmpl_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
app = Flask(__name__, template_folder=tmpl_dir)

# Prompts for the password from the user and connects to the database.

password = input("Password for ns2984: ")
DATABASEURI = "postgresql://ns2984:" + str(password) + "@w4111db1.cloudapp.net:5432/proj1part2"


#
# This line creates a database engine that knows how to connect to the URI above
#
engine = create_engine(DATABASEURI)

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

  cursor = g.conn.execute("SELECT pid, name FROM portfolio")
  pids = []
  names = []
  for result in cursor:
    names.append(result['name'].strip())
    pids.append(result['pid'])
  cursor.close()

  context = dict(names=names, pids=pids)

  #
  # render_template looks in the templates/ folder for files.
  # for example, the below file reads template/index.html
  #
  return render_template("index.html", **context)



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

  # try:
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
  # except:
  #   errors.append("Invalid Order.")
  #   return display_stocks(pid, portfolio, errors) 

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

  cursor_buy_mt = select_market_orders(ticker, "BUY")
  cursor_sell_mt = select_market_orders(ticker, "SELL")

  cursor_buy = select_price_orders(ticker, "BUY")
  cursor_sell = select_price_orders(ticker, "SELL")

  if cursor_buy_mt.rowcount != 0:
    exec_buy_mt(ticker, cursor_buy_mt, cursor_sell)
  if cursor_sell_mt.rowcount != 0:
    exec_sell_mt(ticker, cursor_sell_mt, cursor_buy)

  exec_price_orders(ticker)

def select_market_orders(ticker, otype):
  if otype == "BUY":
    ordering = "DESC"
  else:
    ordering = "ASC"
  return g.conn.execute("SELECT id, trader, portfolio, unit_price," + \
                        "quantity FROM Trade_Order WHERE stock =" + \
                        " %s AND type = %s AND market = TRUE " + \
                        "ORDER BY unit_price " + ordering + ";", ticker, otype)

def select_price_orders(ticker, otype):
  if otype == "BUY":
    ordering = "DESC"
  else:
    ordering = "ASC"
  return g.conn.execute("SELECT id, trader, portfolio, unit_price," + \
                        "quantity FROM Trade_Order WHERE stock =" + \
                        " %s AND type = %s AND market = FALSE ORDER BY " + \
                        "unit_price " + ordering + ";", ticker, otype)

def exec_buy_mt(ticker, cursor_buy_mt, cursor_sell):

  for buy_order in cursor_buy_mt:
    buy_id = int(buy_order['id'])
    buy_user = buy_order['trader'].strip()
    buy_pid = int(buy_order['portfolio'])
    buy_qty = int(buy_order['quantity'])

  if cursor_sell.rowcount == 0:
    delete_market_orders(ticker, "BUY")
    return

  for sell_order in cursor_sell:
    cursor_buy_mt = select_market_orders(ticker, "BUY")
    if cursor_buy_mt.rowcount == 0:
      return

    sell_id = int(sell_order['id'])
    sell_user = sell_order['trader'].strip()
    sell_pid = int(sell_order['portfolio'])
    sell_price = float(sell_order['unit_price'].replace(',', "")[1:])
    sell_qty = int(sell_order['quantity'])

    if not check_assets(sell_pid, "SELL", ticker, False, sell_price, sell_qty):
      delete_order(sell_id)
      continue

    if buy_qty >= sell_qty:
      if not check_assets(buy_pid, "BUY", ticker, True, sell_price, sell_qty):
        delete_market_orders(ticker, "BUY")
        return

      create_transaction(sell_qty, sell_price * sell_qty, buy_user, buy_pid,
                         ticker, 'BUY', datetime.now())

      create_transaction(sell_qty, sell_price * sell_qty, sell_user, sell_pid,
                         ticker, 'SELL', datetime.now())

      # Update the portfolios
      update_portfolio(buy_pid, ticker, sell_qty)
      update_portfolio(sell_pid, ticker, -sell_qty)

      update_cash(buy_pid, -sell_price * sell_qty)
      update_cash(sell_pid, sell_price * sell_qty)

      if buy_qty == sell_qty:
        delete_order(buy_id)
      else:
        update_order(buy_id, buy_qty-sell_qty)
      delete_order(sell_id)
    
    else:
      if not check_assets(buy_pid, "BUY", ticker, True, sell_price, buy_qty):
        delete_market_orders(ticker, "BUY")
        return
      
      create_transaction(buy_qty, sell_price * buy_qty, buy_user, buy_pid,
                         ticker, 'BUY', datetime.now())
      create_transaction(buy_qty, sell_price * buy_qty, sell_user, sell_pid,
                         ticker, 'SELL', datetime.now())

      update_portfolio(buy_pid, ticker, buy_qty)
      update_portfolio(sell_pid, ticker, -buy_qty)

      update_cash(buy_pid, -sell_price * buy_qty)
      update_cash(sell_pid, sell_price * buy_qty)

      update_order(sell_id, sell_qty-buy_qty)
      delete_order(buy_id)
      
    g.conn.execute("UPDATE Stock SET market_price = %s WHERE ticker = %s;", sell_price, ticker)
  
  return True

def exec_sell_mt(ticker, cursor_sell_mt, cursor_buy):

  for sell_order in cursor_sell_mt:
    sell_id = int(sell_order['id'])
    sell_user = sell_order['trader'].strip()
    sell_pid = int(sell_order['portfolio'])
    sell_qty = int(sell_order['quantity'])

  if cursor_buy.rowcount == 0:
    delete_market_orders(ticker, "SELL")
    return

  for buy_order in cursor_buy:
    cursor_sell_mt = select_market_orders(ticker, "SELL")
    if cursor_sell_mt.rowcount == 0:
      return

    buy_id = int(buy_order['id'])
    buy_user = buy_order['trader'].strip()
    buy_pid = int(buy_order['portfolio'])
    buy_price = float(buy_order['unit_price'].replace(',', "")[1:])
    buy_qty = int(buy_order['quantity'])

    if not check_assets(buy_pid, "BUY", ticker, False, buy_price, buy_qty):
      delete_order(buy_id)
      continue

    if sell_qty >= buy_qty:
      if not check_assets(sell_pid, "SELL", ticker, True, buy_price, buy_qty):
        delete_market_orders(ticker, "SELL")
        return

      create_transaction(buy_qty, buy_price * buy_qty, sell_user, sell_pid,
                         ticker, 'SELL', datetime.now())

      create_transaction(buy_qty, buy_price * buy_qty, buy_user, buy_pid,
                         ticker, 'BUY', datetime.now())

      # Update the portfolios
      update_portfolio(sell_pid, ticker, buy_qty)
      update_portfolio(buy_pid, ticker, -buy_qty)

      update_cash(buy_pid, -buy_price * buy_qty)
      update_cash(sell_pid, buy_price * buy_qty)

      if buy_qty == sell_qty:
        delete_order(sell_id)
      else:
        update_order(sell_id, sell_qty-buy_qty)
      delete_order(buy_id)
    
    else:
      if not check_assets(sell_pid, "SELL", ticker, True, buy_price, sell_qty):
        delete_market_orders(ticker, "SELL")
        return
      
      create_transaction(sell_qty, buy_price * sell_qty, sell_user, sell_pid,
                         ticker, 'SELL', datetime.now())
      create_transaction(sell_qty, buy_price * sell_qty, buy_user, buy_pid,
                         ticker, 'BUY', datetime.now())

      update_portfolio(sell_pid, ticker, sell_qty)
      update_portfolio(buy_pid, ticker, -sell_qty)

      update_cash(buy_pid, -buy_price * sell_qty)
      update_cash(sell_pid, buy_price * sell_qty)

      update_order(buy_id, buy_qty-sell_qty)
      delete_order(sell_id)
      
    g.conn.execute("UPDATE Stock SET market_price = %s WHERE ticker = %s;", buy_price, ticker)
  return True

def exec_price_orders(ticker):
  while True:
    cursor_buy = select_price_orders(ticker, "BUY")
    cursor_sell = select_price_orders(ticker, "SELL")

    if cursor_buy.rowcount == 0 or cursor_sell.rowcount == 0:
      return

    for buy_order in cursor_buy:
      buy_id = int(buy_order['id'])
      buy_user = buy_order['trader'].strip()
      buy_pid = int(buy_order['portfolio'])
      buy_price = float(buy_order['unit_price'].replace(',', "")[1:])
      buy_qty = int(buy_order['quantity'])
      break

    for sell_order in cursor_sell:
      sell_id = int(sell_order['id'])
      sell_user = sell_order['trader'].strip()
      sell_pid = int(sell_order['portfolio'])
      sell_price = float(sell_order['unit_price'].replace(',', "")[1:])
      sell_qty = int(sell_order['quantity'])
      break

    if not check_assets(sell_pid, "SELL", ticker, False, sell_price, sell_qty):
      delete_order(sell_id)
      continue

    if not check_assets(buy_pid, "BUY", ticker, False, buy_price, buy_qty):
      delete_order(buy_id)
      continue

    if buy_price < sell_price:
      return

    if buy_qty >= sell_qty:
      create_transaction(sell_qty, sell_price * sell_qty, buy_user, buy_pid,
                         ticker, 'BUY', datetime.now())

      create_transaction(sell_qty, sell_price * sell_qty, sell_user, sell_pid,
                         ticker, 'SELL', datetime.now())

      # Update the portfolios
      update_portfolio(buy_pid, ticker, sell_qty)
      update_portfolio(sell_pid, ticker, -sell_qty)

      update_cash(buy_pid, -sell_price * sell_qty)
      update_cash(sell_pid, sell_price * sell_qty)

      if buy_qty == sell_qty:
        delete_order(buy_id)
      else:
        update_order(buy_id, buy_qty-sell_qty)
      delete_order(sell_id)

    else:      
      create_transaction(buy_qty, sell_price * buy_qty, buy_user, buy_pid,
                         ticker, 'BUY', datetime.now())
      create_transaction(buy_qty, sell_price * buy_qty, sell_user, sell_pid,
                         ticker, 'SELL', datetime.now())

      update_portfolio(buy_pid, ticker, buy_qty)
      update_portfolio(sell_pid, ticker, -buy_qty)

      update_cash(buy_pid, -sell_price * buy_qty)
      update_cash(sell_pid, sell_price * buy_qty)

      update_order(sell_id, sell_qty-buy_qty)
      delete_order(buy_id)
      
    g.conn.execute("UPDATE Stock SET market_price = %s WHERE ticker = %s;", sell_price, ticker)

def update_cash(pid, cash):
  cursor_stocks = g.conn.execute("SELECT cash FROM Portfolio WHERE " + \
                                 "pid = %s;", pid)
  for portfolio in cursor_stocks:
    oldcash = float(portfolio['cash'].replace(',', "")[1:])
    newcash = cash + oldcash

  g.conn.execute("UPDATE Portfolio SET cash = %s WHERE pid = %s;",
                  newcash, pid)


def update_portfolio(pid, ticker, quantity):
  cursor_stocks = g.conn.execute("SELECT  quantity FROM Portfolio_Stock WHERE " + \
                                 "portfolio = %s and stock = %s;", pid, ticker)
  if cursor_stocks.rowcount == 0:
    if quantity > 0:
      g.conn.execute("INSERT INTO Portfolio_Stock VALUES (%s, %s, %s);", pid, ticker, quantity)
  else:
    for stock in cursor_stocks:
      newqty = stock['quantity'] + quantity
      if newqty == 0:
        g.conn.execute("DELETE FROM Portfolio_Stock WHERE portfolio = %s " + \
                       "AND stock = %s;", pid, ticker)
        return
      g.conn.execute("UPDATE Portfolio_Stock SET quantity = %s WHERE portfolio = %s and stock = %s;",
                      newqty, pid, ticker)

def update_order(oid, qty):
  g.conn.execute("UPDATE Trade_Order SET quantity = %s WHERE id = %s;",
                 qty, oid) 

def delete_order(oid):
  g.conn.execute("DELETE FROM Trade_Order WHERE id = %s;", oid)

def delete_market_orders(ticker, otype):
  g.conn.execute("DELETE FROM Trade_Order WHERE market = TRUE AND stock " + \
               "= %s AND type = %s", ticker, otype)

def create_transaction(quantity, total_value, trader, portfolio, stock, ttype, timestamp):
    g.conn.execute("INSERT INTO Transaction(quantity, total_value, trader" + \
                   ", portfolio, stock, type, timestamp) VALUES (%s,%s," + \
                   "%s,%s,%s,%s,%s);", quantity, total_value, trader, portfolio,
                   stock, ttype, timestamp)
    return True

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
