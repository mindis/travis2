# Databricks notebook source
from __future__ import print_function                                                                                           # General
from six.moves.urllib.parse import urlencode
import random                                                                                                                   # General/Turnit: Random tall på slutten av temp-tabeller ved upserting
import os
import datetime                                                                                                                 # General
from datetime import datetime, date, timedelta                                                                                  # General
from dateutil.relativedelta import relativedelta                                                                                # Turnit: Definering av tidsparametere
import pandas as pd                                                                                                             # General
import operator
import multiprocessing
import logging                                                                                                                  # General

import calendar
import time
from dateutil import parser
from dateutil import tz                                                                                                         # General
import sys
import re
from itertools import groupby
from multiprocessing.pool import ThreadPool
from time import sleep
import urllib
import base64

### Spark-bibliotek
from pyspark.sql import * ### <---- breaks python built-in functions like "max" , "round", etc                                  # General: i add_metadata-funksjonen trengs current_timestamp-funksjonen, i definering av datatyper i en dataframe trengs .types o.l.
from pyspark.sql import Column as col                                                                                           # General
from pyspark.sql.types import *                                                                                                 # General
import pyspark.sql.functions as F                                                                                               # General: Ideell, men importerer * i general da alle funksjoner må endres til prefiks F. hvis vi infører denne (se kommentar i imports_general)
from pyspark.sql.functions import *                                                                                             # General
from pyspark.sql.window import *
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql import DataFrame                                                                                               # General: Slack


import six.moves.builtins
import boto
from boto.s3.connection import S3Connection
import boto.ec2
import boto3
import boto3.session
from six import StringIO
from six import iteritems
from six import itervalues
import json
from socket import error as SocketError
import errno
import requests                                                                                                                 # Turnit: Mapping av Linjenavn og Linjenummer mot en mappingtabell i Sharepoint
from requests.auth import HTTPBasicAuth                                                                                         # Turnit
from requests_ntlm import HttpNtlmAuth                                                                                          # Turnit
from lxml import etree
from lxml.html import fromstring as parsehtml
import pytz
import psycopg2                                                                                                                 # General: Kreves av Redshift upsert grunnet kall til rs_command | Turnit: tilkobling = psycopg2.connect(dbname = env('REDSHIFT_DATABASE'), host =  env('REDSHIFT_SERVER'),port = env('REDSHIFT_PORT'))
from psycopg2.extras import DictCursor                                                                                          # Turnit
from bs4 import BeautifulSoup

### Importerer Zeep for SOAP 
import zeep

import py4j                                                                                                                     # General: For error handling
import copy
