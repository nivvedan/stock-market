#!/usr/bin/env python2.7

"""
Handling Market orders

"""
import os

from datetime import datetime
from sqlalchemy import *
from sqlalchemy.pool import NullPool
from flask import Flask, request, render_template, g, redirect, Response

# execute market buy order
def exec_buy_mt(ticker, cursor_buy_mt, cursor_sell):

	buy_price = 0
	buy_qty = 0
	buy_id = 0
	buy_pid = 0
	buy_user = ''
	for buy_order in cursor_buy_mt:
		buy_id = buy_order['id']
		buy_user = buy_order['trader']
		buy_pid = buy_order['portfolio']
		buy_price = buy_order['unit_price']
		buy_qty = buy_order['quantity']
		break

	sell_price = 0
	sell_qty = 0
	sell_id = 0
	sell_pid = 0
	sell_user = ''
	for sell_order in cursor_sell:
		sell_id = sell_order['id']
		sell_user = sell_order['trader']
		sell_pid = sell_order['portfolio']
		sell_price = buy_order['unit_price']
		sell_qty = buy_order['quantity']
		break

	# create transaction id (tid)
	tran_cursor = g.conn.execute("SELECT tid FROM Transaction ORDER BY tid DESC LIMIT 1;")
	max_tid = 0
	for tran in tran_cursor:
		max_tid = tran['tid']
		break

	qty_diff = buy_qty - sell_qty
	if qty_diff >= 0:
		g.conn.execute("INSERT INTO Transaction VALUES (%s,%s,%s,%s,%s,%s,%s,%s);",
																										max_tid + 1, sell_qty, sell_price * sell_qty, buy_user,
																										buy_pid, ticker, 'BUY', datetime.now())
		g.conn.execute("INSERT INTO Transaction VALUES (%s,%s,%s,%s,%s,%s,%s,%s);",
																										max_tid + 2, sell_qty, sell_price * sell_qty, sell_user,
																										sell_pid, ticker, 'SELL', datetime.now())

		# UPDATING BUY PORTFOLIO check if stock already exists in Portfolio_Stock
		cursor_stocks = g.conn.execute("SELECT * FROM Portfolio_Stock WHERE portfolio = %s and ticker = %s;", buy_pid, ticker)
		if cursor_stocks.rowcount == 0:
			g.conn.execute("INSERT INTO Portfolio_Stock VALUES (%s, %s, %s);", buy_pid, ticker, quantity)
		else:
			for stock in cursor_stocks:
				g.conn.execute("UPDATE FROM Portfolio_Stock SET quantity = %s WHERE portfolio = %s and ticker = %s;",
												stock['quantity'] + sell_qty, buy_pid, ticker)
				break

		# UPDATING SELL PORTFOLIO check if stock already exists in Portfolio_Stock
		cursor_stocks = g.conn.execute("SELECT * FROM Portfolio_Stock WHERE portfolio = %s and ticker = %s;", sell_pid, ticker)
		for stock in cursor_stocks:
			if stock['quantity'] == sell_qty:
				g.conn.execute("DELETE FROM Portfolio_Stock WHERE portfolio = %s and ticker = %s;", sell_pid, ticker)
			else:
				g.conn.execute("UPDATE FROM Portfolio_Stock SET quantity = %s - %s WHERE portfolio = %s and ticker = %s;",
												stock['quantity'] - sell_qty, sell_pid, ticker)
			break		
		g.conn.execute("DELETE FROM Trade_Order WHERE id = %s;", sell_id)
	
	else:  # sell quantity is more 
		g.conn.execute("INSERT INTO Transaction VALUES (%s,%s,%s,%s,%s,%s,%s,%s);",
																										max_tid + 1, buy_qty, sell_price * buy_qty, buy_user,
																										buy_pid, ticker, 'BUY', datetime.now())
		g.conn.execute("INSERT INTO Transaction VALUES (%s,%s,%s,%s,%s,%s,%s,%s);",
																										max_tid + 2, buy_qty, sell_price * buy_qty, sell_user,
																										sell_pid, ticker, 'SELL', datetime.now())
	
		# UPDATING BUY PORTFOLIO check if stock already exists in Portfolio_Stock
		cursor_stocks = g.conn.execute("SELECT * FROM Portfolio_Stock WHERE portfolio = %s and ticker = %s;", buy_pid, ticker)
		if cursor_stocks.rowcount == 0:
			g.conn.execute("INSERT INTO Portfolio_Stock VALUES (%s, %s, %s);", buy_pid, ticker, quantity)
		else:
			for stock in cursor_stocks:
				g.conn.execute("UPDATE FROM Portfolio_Stock SET quantity = %s WHERE portfolio = %s and ticker = %s;",
												stock['quantity'] + sell_qty, buy_pid, ticker)
				break

		# UPDATING SELL PORTFOLIO check if stock already exists in Portfolio_Stock
		cursor_stocks = g.conn.execute("SELECT * FROM Portfolio_Stock WHERE portfolio = %s and ticker = %s;", sell_pid, ticker)
		for stock in cursor_stocks:
			if stock['quantity'] == sell_qty:
				g.conn.execute("DELETE FROM Portfolio_Stock WHERE portfolio = %s and ticker = %s;", sell_pid, ticker)
			else:
				g.conn.execute("UPDATE FROM Portfolio_Stock SET quantity = %s - %s WHERE portfolio = %s and ticker = %s;",
												stock['quantity'] - sell_qty, sell_pid, ticker)
			break		
		g.conn.execute("UPDATE FROM Trade_Order SET quantity = %s - %s WHERE id = %s;", sell_qty, buy_qty, sell_id)
	
	g.conn.execute("UPDATE FROM Stock SET market_price = %s WHERE ticker = %s;", sell_price, ticker)
	g.conn.execute("DELETE FROM Trade_Order WHERE id = %s;", buy_id)
	return True
	

# execute market buy order
def exec_sell_mt(ticker, cursor_sell_mt, cursor_buy):
	return True

# # execute market buy order
# def exec_buy_mt(cursor_buy_mt, cursor_sell):
	
# 	buy_price = 0
# 	buy_qty = 0
# 	for buy_order in cursor_buy_mt:
# 		buy_price = buy_order['unit_price']
# 		buy_qty = buy_order['quantity']

# 	# Find sell orders
# 	for sell_order in cursor_sell:
# 		price_diff = buy_price - sell_order['unit_price']
# 		qty_diff = buy_qty - sell_order['quantity']

# 		if price_diff < 0:
# 			g.conn.execute("DELETE FROM Trade_Order WHERE market = True and type = BUY;")
# 			break
# 		elif buy_qty > 0 and price_diff >= 0:
# 			# BUY ORDER 
# 			g.conn.execute("UPDATE FROM Trade_Order SET quantity = %s - %s WHERE id = %s;", buy_qty, qty_diff, buy_order['pid'], 
# 				buy_order['quantity'], buy_order['unit_price'])
       
# 			# SELL ORDER

# 		elif buy_qty = 0 and price_diff >= 0:
# 			# BUY ORDER 

# 			# SELL ORDER

# 		elif buy_qty < 0 and price_diff >= 0:
# 			# BUY ORDER 

# 			# SELL ORDER


# 	return True

# # execute market sell order
# def exec_sell_mt(cursor_sell_mt, cursor_buy):
	
# 	return True


# 	for sell_order in cursor_sell_mt:
#       g.conn.execute("DELETE FROM Trade_Order WHERE pid = %s and quantity = %s and unit_price = %s and type = SELL;",
#                             sell_order['pid'], sell_order['quantity'], sell_order['unit_price'])
#     for sell_order in cursor_sell_mt:
#       g.conn.execute("DELETE FROM Trade_Order WHERE pid = %s and quantity = %s and unit_price = %s and type = BUY;",
#                             buy_order['pid'], buy_order['quantity'], buy_order['unit_price'])
