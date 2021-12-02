"""py file to test multiple tests action"""

import os

for test in os.listdir('./test/'):

    if test != 'run_all_python.py' and test != 'requirements.txt':
        os.system('python3 ./test/' + test)
