from collections import defaultdict

text = """
Nous allons donc supposer que nos données d'entrée ont été découpées 
en différents fragments et qu'une opération de simplification a été 
appliquée sur chaque fragment pour supprimer les caractères de ponctuation, 
transformer chaque mot en son singulier ("nos" devient "notre") 
et ne garder que les mots de plus de 3
"""


def count_words(text):
    dic = defaultdict(int)
    for word in text.split():
        dic[word.lower()] += 1
    return dic


# print(list(count_words(text).items()))

D1 = {"./lot1.txt": "jour lève notre grisaille"}
D2 = {"./lot2.txt": "trottoir notre ruelle notre tour"}
D3 = {"./lot3.txt": "jour lève notre envie vous"}
D4 = {"./lot4.txt": "faire comprendre tous notre tour"}


def map(key, value):
    # return list(count_words(lot).items())
    intermediate = []
    for word in value.split():
        intermediate.append((word, 1))
    return intermediate


def recure_sum(l):
    if not l:
        return 0
    if len(l) == 1:
        return l[0]
    return l[0] + recure_sum(l[1:])


print(recure_sum([3, 6, 7, 9, 0]))


# understand and write Pagerank


def map(key, value):
    intermediate = []
    for row in value:
        intermediate.append((row[0], (row[1], row[1:])))
    return intermediate
