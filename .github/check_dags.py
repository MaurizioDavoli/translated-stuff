"""for action scope"""
import os


for test in os.listdir('./dags/'):
    os.system('pylint ./dags/' + test)
