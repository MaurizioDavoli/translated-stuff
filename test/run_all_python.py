"""py file to test multiple tests action"""

import os

for test in os.listdir('./test/'):

    if test != 'run_all_python.py':
        print(test)

    # os.system('python3 '+test)
