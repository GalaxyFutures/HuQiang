from WindPy import *
w.start()
data = w.wsd("600000.SH", "open,high,low,close", "2016-05-28", "2016-06-26", "")
print(data)