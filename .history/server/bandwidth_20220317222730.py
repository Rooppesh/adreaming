import psutil

# value = psutil.net_io_counters().bytes_sent + psutil.net_io_counters().bytes_recv
value = psutil.net_io_counters(pernic=False)
# bandwidth = value/1024./1024.*8
print(value[0]/1024./1024.)