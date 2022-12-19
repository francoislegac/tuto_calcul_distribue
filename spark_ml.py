from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml import Pipeline

sc = SparkContext()
spark = SparkSession(sc)


def load_dataframe(path):
    rdd = (
        sc.textFile(path)
        .map(lambda line: line.split())
        .map(lambda words: Row(label=words[0], words=words[1:]))
    )
    return spark.createDataFrame(rdd)


train_data = load_dataframe("20ng-train-all-terms.txt")
test_data = load_dataframe("20ng-test-all-terms.txt")

vectorizer = CountVectorizer(inputCol="words", outputCol="bag_of_words")
label_indexer = StringIndexer(inputCol="label", outputCol="label_index")
classifier = NaiveBayes(
    labelCol="label_index",
    featuresCol="bag_of_words",
    predictionCol="label_index_predicted",
)

pipeline = Pipeline(stages=[vectorizer, label_indexer, classifier])
pipeline_model = pipeline.fit(train_data)

test_predicted = pipeline_model.transform(test_data)

evaluator = MulticlassClassificationEvaluator(
    labelCol="label_index", predictionCol="label_index_predicted", metricName="accuracy"
)
accuracy = evaluator.evaluate(test_predicted)

print(f"Accuracy = {accuracy:.2f}")

input("press ctrl+c to exit")
