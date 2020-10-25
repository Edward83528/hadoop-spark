# -*- coding: utf-8 -*-
from pyspark import SparkConf, SparkContext

# 建立SparkConf
conf = SparkConf().setMaster("local[*]").setAppName("mySparkInvertedIndex")
sc = SparkContext(conf=conf)

rdd = sc.wholeTextFiles("data/*.txt") # 讀取多個檔案，創建RDD(包含檔案路徑)
print(rdd.count())
#print(rdd.first())
#print(rdd)
print(type(rdd))

# 單詞分割 flatMap 後 依照(檔案路徑,單詞) 再map倒過來 (單詞,檔案路徑)最後依照相同key來知道單詞出現在哪些檔案
# 最後一行將檔案路徑的字串轉陣列使得 跟投影片圖片一樣 >並我最後再做個排序
invertedIndex_rdd=rdd.flatMap(lambda fc: ((fc[0], s) for s in fc[1].lower().split()))\
                .map(lambda x: (x[1],x[0]))\
                .reduceByKey(lambda a,b: a +","+ b if b not in a else a )\
                .map(lambda x: (x[0],x[1].split(',')) )\
                .sortByKey();

print(invertedIndex_rdd.take(10))

#保存結果到hdfs文件存儲系統中
invertedIndex_rdd.coalesce(1).saveAsTextFile("output");
sc.stop() # 停止 SparkContext
