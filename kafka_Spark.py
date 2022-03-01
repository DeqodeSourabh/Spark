from pyspark.context import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
if __name__=="__main__":
    sc=SparkContext(appName = "kafka Spark")
    ssc = StreamingContext(sc, 60)
    message= KafkaUtils.createDirectStream(ssc,topics =['testtopic'],kafkaParams= {'metadata.broker.list': "localhost:9092"})
    words = message.map(lambda x: x[1]).flatmap(lambda x: x.split(" "))
    wordcount = words.map(lambda x: (x,1)).reduceByKey(lambda a,b: a+b)

    wordcount.pprint()
    ssc.start()
    ssc.awaitTermination()