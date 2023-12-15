#run in pyspark
#sc is the spark context
import os 
dir = '/content/dataset'
files = os.listdir(dir)
dict = {year:{date:{country:float('nan') for country in countries} for date in dates} for year in years}
counter = 1
data = sc.emptyRDD()
for file in files:
  if '.csv' in file:
    print(file)
    print("Counter: "+str(counter))
    counter = counter+1
    year = file.split('.')[0]
    data = data.union(sc.textFile('/content/dataset/'+file).map(lambda x: x.split(',')[0:4]))
T = data.filter(lambda x: x[2][0:2]=='TM' and x[3]<=700)
T = T.map(lambda x: (x[1]+x[0][0:2],int(x[3])))
counts = T.map(lambda x: (x[0],1)).reduceByKey(lambda a,b:a+b)
T = T.reduceByKey(lambda a,b: a+b)
T = T.join(counts)
T = T.map(lambda x: (x[0],x[1][0]/x[1][1]/20))
temp_summary = T.collect()
for summary in temp_summary:
  length = len(summary[0])
  date = summary[0][4:length-2]
  year = summary[0][0:4]
  country = summary[0][length-2:length]
  temp_dict[year][date][country]=summary[1]
