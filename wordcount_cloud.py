import sys
from pyspark import SparkContext

sc = SparkContext()
word_counts = (
    sc.textFile(sys.argv[1])
    .flatMap(lambda line: line.split(" "))
    .map(lambda word: (word, 1))
    .reduceByKey(lambda count1, count2: count1 + count2)
    .takeOrdered(50, lambda items: (items[1]))
)  # 0:word, 1: count

# log results to s3
"""
import json
import boto.s3, boto.s3.key

conn = boto.s3.connect_to_region("eu-west-1")
bucket = conn.get_bucket("flg-oc-calculsdistribues")
key = boto.s3.key.Key(bucket, "words.txt")


key.set_contents_from_string(json.dump(word_counts, indent=2))
"""
for w in word_counts:
    print(w)
