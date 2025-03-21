
import pandas
import json
import sys
import os
import io
import psycopg2
import optuna
import subprocess
import verstack
import threading
import pathlib
import dataclasses
import sqlalchemy
import warnings
import lightgbm
import selenium
import multiprocessing
import socket
import asyncio
import abc
import time
import cryptography
import datetime
import uuid
import random
import numpy
import unicodedata
import sched
import concurrent
import decimal
import tracemalloc
import requests
import timebudget
import unidecode
import twilio
import flask
import boto3
import pendulum
import tqdm
import xlrd
import pytest
import apscheduler
import blinker
import bokeh
import certifi
import fastparquet
import mysql
import sklearn
import pyspark
import xlsxwriter
import pickle
import logging
import inspect
import atexit

class Logger():

    def getCurrentFrames(self):
        curframe = inspect.currentframe()
        self.calframe = inspect.getouterframes(curframe, 2)

    def formatLogFilePath(self):
        head_tail = os.path.split(self.calframe[2][1])
        self.fname = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs', head_tail[1].split('.')[0]+'.log')
    
    def log(self):
        self.getCurrentFrames()
        self.formatLogFilePath()
        self.setLog()
    
    def exit(self):
        logging.info(str(f'Exiting at {datetime.datetime.now()}'))

    def setLog(self):
        logging.basicConfig(filename=f'{self.fname}', encoding='utf-8', level=logging.DEBUG)
        logging.info(str(f'Starting at {datetime.datetime.now()}'))
        atexit.register(self.exit)

if __name__ == '__main__':
    assert os.system("python3.11 -V") == 0
    assert os.system("pip3.11 list") == 0
    

