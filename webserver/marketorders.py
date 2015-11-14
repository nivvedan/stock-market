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
	for buy_order in cursor_buy_mt:
		buy_id = buy_order['id']
		buy_pid = buy_order['portfolio']
		buy_price = buy_order['unit_price']
		buy_qty = buy_order['quantity']
		break

	sell_price = 0
	sell_qty = 0
	sell_id = 0
	sell_pid = 0
	for sell_order in cursor_sell:
		sell_id = sell_order['id']
		sell_pid = sell_order['portfolio']
		sell_price = buy_order['unit_price']
		sell_qty = buy_order['quantity']
		break

	qty_diff = buy_qty - sell_qty
	if qty_diff >= 0:
		g.conn.execute("UPDATE FROM Stock SET market_price = %s WHERE ticker = %s;", sell_price, ticker)
	else:
		#g.conn.execute("DELETE FROM Trade_Order WHERE id = %s;", buy_id)
		g.conn.execute("UPDATE FROM Stock SET market_price = %s WHERE ticker = %s;", sell_price, ticker)
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