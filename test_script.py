
# coding: utf-8

# In[5]:

import datetime
f = open('testfile.txt', 'w')
f.write('This is a test\n %s' % datetime.datetime.now())
f.close()

