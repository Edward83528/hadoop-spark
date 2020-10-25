# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext

# 建立SparkConf
conf = SparkConf().setMaster("local[*]").setAppName("mySparkWordCount")
sc = SparkContext(conf=conf)

rdd = sc.textFile("data/*.txt") # 讀取多個檔案，創建RDD
print(rdd.count())
#print(rdd.first())
#print(rdd)
print(type(rdd))

# flatMap會取出list的東西
# 改成sortBy 依照自己需要的規則排序(在這會依照字數做降序)
word_count_rdd=rdd.\
                flatMap(lambda line: line.split(" ")).\
                map(lambda word: (word, 1)).\
                reduceByKey(lambda a, b: a + b).\
                sortBy(lambda x:x[1],ascending=False);

print(word_count_rdd.collect())

#保存結果到hdfs文件存儲系統中
word_count_rdd.coalesce(1).saveAsTextFile("output");

sc.stop() # 停止 SparkContext
