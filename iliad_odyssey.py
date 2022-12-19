from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext()
spark = SparkSession(sc)


def filter_stop_words(partition):
    from nltk.corpus import stopwords

    english_stop_words = set(stopwords.words("english"))
    for word in partition:
        if word not in english_stop_words:
            yield word


def get_word_freq(path: str):
    vocabulary = (
        sc.textFile(path, minPartitions=6)
        .flatMap(lambda line: line.split())
        .flatMap(lambda word: word.split("."))
        .flatMap(lambda word: word.split(","))
        .flatMap(lambda word: word.split("!"))
        .flatMap(lambda word: word.split("?"))
        .flatMap(lambda word: word.split("'"))
        .flatMap(lambda word: word.split('"'))
        .filter(lambda word: word is not None and len(word) > 0)
        .mapPartitions(filter_stop_words)
        .persist()
    )

    word_count = vocabulary.count()

    word_freq = (
        vocabulary.map(lambda word: (word, 1))
        .reduceByKey(lambda count1, count2: count1 + count2)
        .map(lambda word_and_count: (word_and_count[0], word_and_count[1] / word_count))
    )

    return word_freq


def main():
    iliad = get_word_freq("iliad.mb.txt")
    odyssey = get_word_freq("odyssey.mb.txt")

    join_words = iliad.fullOuterJoin(odyssey).map(
        lambda items: (
            items[0],
            (items[1][0] or 0) - (items[1][1] or 0),
        )  # 0:word, 10:freq1, 11:freq2
    )

    emerging_words = join_words.takeOrdered(
        10, lambda items: -items[1]
    )  # 0:word, 1:freq_diff
    disappearing_words = join_words.takeOrdered(10, key=lambda items: items[1])
    for word, freq_diff in emerging_words:
        print(f"{freq_diff*10000}, {word}")
    for word, freq_diff in disappearing_words[::-1]:
        print(f"{freq_diff*10000}, {word}")

    input("press ctrl+c to exit")


if __name__ == "__main__":
    main()


""""
from collections import defaultdict
with open("iliad.mb.txt") as f:
    text = f.read()
def word_count(text):
    dic = defaultdict(int)
    for word in text.split():
        dic[word] += 1
    return dic
"""
