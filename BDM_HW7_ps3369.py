from pyspark import SparkContext
from pyspark.sql import HiveContext
from datetime import timedelta
from datetime import date, datetime
from geopy.distance import vincenty


def filt(splitInd, iterator): 
    if splitInd == 0:
        iterator.next()
    import csv
    reader = csv.reader(iterator)
    for row in reader:
        if (row[3][:10] == '2015-02-01') & (row[6] == 'Greenwich Ave & 8 Ave'):
            dt = datetime.strptime(row[3], "%Y-%m-%d %H:%M:%S+%f")
            yield dt, row[0]
            
def filt1(splitInd, iterator):
    if splitInd == 0:
        iterator.next()
    import csv
    reader = csv.reader(iterator)
    for row in reader:
        if (row[4] != 'NULL') & (row[5] != 'NULL'):
            if (vincenty((40.73901691,-74.00263761), (float(row[4]), float(row[5]))).miles) <= 0.25:
                dt = datetime.strptime(row[1], "%Y-%m-%d %H:%M:%S.%f")
                del_t = dt + timedelta(seconds = 600)
                yield dt,del_t

sc = SparkContext()                
spark = HiveContext(sc)

yellow_data = sc.textFile('/tmp/yellow.csv.gz').cache()
citibike_data = sc.textFile('/tmp/citibike.csv').cache()

citi = citibike_data.mapPartitionsWithIndex(filt)
yell = yellow_data.mapPartitionsWithIndex(filt1)

citi_df = citi.toDF(['time_started', 'ride'])
yellow_df = yell.toDF(['time_drop', 'time_extend'])

d = yellow_df.join(citi_df).filter((yellow_df.time_drop < citi_df.time_started) & (yellow_df.time_extend > citi_df.time_started))

print len(d.toPandas()['ride'].unique())