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
def exec_sell_mt(cursor_buy_mt, cursor_sell):
	
	return True

# execute market sell order
def exec_sell_mt(cursor_sell_mt, cursor_buy):
	
	return True


	for sell_order in cursor_sell_mt:
      g.conn.execute("DELETE FROM Trade_Order WHERE pid = %s and quantity = %s and unit_price = %s and type = SELL;",
                            sell_order['pid'], sell_order['quantity'], sell_order['unit_price'])
    for sell_order in cursor_sell_mt:
      g.conn.execute("DELETE FROM Trade_Order WHERE pid = %s and quantity = %s and unit_price = %s and type = BUY;",
                            buy_order['pid'], buy_order['quantity'], buy_order['unit_price'])