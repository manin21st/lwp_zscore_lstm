import cx_Oracle
import os

os.environ["PATH"] = r"C:\instantclient_18_5;" + os.environ["PATH"]
print("Oracle Client Version:", cx_Oracle.clientversion())
