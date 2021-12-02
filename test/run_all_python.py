"""py file to test multiple tests action"""

import os

for test in os.listdir('./test/'):
    print(str(test))
    # os.system('python3 '+test)
