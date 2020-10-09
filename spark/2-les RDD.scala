// Les RDDs

// 6.2 Apprendre les bases de la programmation des RDDs
// 6.3 map, flatMap, filter, distinct, groupBy et sample
// 6.4 Union, intersection, substract, cartesian, reduce et fold
// 6.5 Pair Rdd: groupByKey, reduceByKey, mapValues, keys, values et sortByKey
// 6.6 Join, rightOuterJoin, leftOuterJoin et cogroup
// 6.7 Exercice

// 6.2 Apprendre les bases de la programmation des RDDs

// SparkContext(sc) permet de créer des RDDs
// 1 à partir d'une source de données
// 2 en parallélisant une collction
// 3 en appliquant une opération de transfo

// méthode 1

//local
val rdd1 = sc.textFile("/tmp/monFichier.txt")

//Hadoop
val rdd1 = sc.textFile("hdfs://.../tmp/monFichier.txt") // on passe un fichier hdfs

// méthode 2

//Création d'une liste
val liste = List("vert", "rouge", "bleu")
val simple_rdd = sc.parallelize(liste)

// méthode 3

val liste = List("vert", "rouge", "bleu")
val simple_rdd = sc.parallelize(list
val rdd2 = simple_rdd.map(x => (x,1))

// RDD immutable, non modifiable
// il est lazy: contient des ref aux données, il attend l'action
// cashable: peut être sauvegardé

// Lors de sa création le RDD est partionné
// Le nombre de partitions dépend de:
// nombre de worker disponibles
// config du cluster

// Les partitions sont parfois imposées, HDFS avec le principe de partition par blocs

// Par défaut
// Autant de core que la machine a, sinon blocs HDFS

val distFile1 = sc.textFile("petitFichier")  
distFile1.getNumPartitions

val distFile2 = sc.textFile("grosFichier") 
distFile2.getNumPartitions

// Les opérations sur RDD

// Les transformations sont lazy
// Les actions: un RDD va être évalué une fois que l'action est exécutée

// https://training.databricks.com/visualapi.pdf

val rdd = sc.textFile('chemin/ttt.txt') //RDD créé

rdd.count() // count() est une action, chargement des données



// 6.3 map, flatMap, filter, distinct, groupBy et sample

val rdd = sc.textFile("/Users/julide/Desktop/projets python/spark/ressources/demo.txt")

rdd.collect //affiche le contenu du RDD et chaque ligne est un élément dan sun tableau

val capital = rdd.map(x => x.toUpperCase())

val rdd = sc.parallelize(List(List(1,2), List(3,4)))

rdd.map(x => x).collect

rdd.flatMap(x => x).collect // ne print pas en liste mais met tout dans un seul tableau

val rdd = sc.parallelize(List("Pierre", "Alpha", "Medhi", "Jean-Pierre", "Thierry"))
val rddPrenomComposes = rdd.filter(x => x.contains("-"))
rddPrenomComposes.collect

val rdd = sc.parallelize(List("Pierre", "Alpha", "Medhi", "Jean-Pierre", "Thierry", "Pierre"))
val rddDistinct = rdd.distinct()
rddDistinct.collect
val rddGroupBy = rdd.groupBy(x => x.charAt(0))

val rdd = sc.parallelize(1 to 100000)
rdd.count
val sampledData = rdd.sample(false, 0.5) // false = que des valeurs uniques, pas de doublons
// 0,5: je dois récupérer 50% + ou - de mon échantillon
sampledData.count
sampledData.take(30) // on regarde les 30 premiers éléments 

// 6.4 Union, intersection, substract, cartesian, reduce et fold

val rdd1 = sc.parallelize(List(1,2,3,4))
val rdd2 = sc.parallelize(List(3,4,5,6))
rdd1.union(rdd2).collect
rdd1.intersection(rdd2).collect
rdd1.subtract(rdd2).collect
rdd1.cartesian(rdd2).collect

val rdd = sc.parallelize(1 to 5)
rdd.collect
rdd.reduce((x,y) => x+y) // donne 15 

val rdd = sc.parallelize(1 to 5, 2) // partitions
rdd.glom().collect // un tableau de deux tableaux
rdd.reduce((x,y) => x+y) // impossible car va faire (1-2) - (3-4-5)
rdd.fold(9)((x,y) => x+y) // il va sommer les partitions, et va ajouter 9 à chaque partition
// (9+ (9+ (1+2)) +(9+ (3+4+5)))
// (9+ (0+ (3)) + (9+ (12)))
// (9+ (12) + (21))
// (9 + 33)
// 42

// 6.5 Pair Rdd: groupByKey, reduceByKey, mapValues, keys, values et sortByKey

val rdd = sc.parallelize(List("cle1 valeur1", "cle2 valeur2", "cle3 valeur3"))

// quelle valeur associer à la clé?
// quel valeur associer à la clé?
// solution 1
val rddPair1 = rdd.map(x => (x.split(" ")(0), x.split(" ")(1)))

val rddCV = sc.parallelize(List("c1 v1", "c2 v2", "c3 v3"))
val rddP = rddCV.map(x => x.split(" ")) // tableau de tableaux avec les éléments par 2
val rddP2 = rddP.map(x => (x(0),x(1)))
rddpP2.collect // un tableau de tupes clé valeur

val rddP3 = rddCV.map(x => (x.split(" ")(0), x.split(" ")(1)))

// solution 2
val rddpair2 = rdd.keyBy(x => x.split(" ")(0))
rddPair2.collect

val rdd  sc.parallelize(List("0,11", "1,11", "0,4", "2,8", "1,1", "9,8"))
val rddPair = rdd.map( x=> (x.split(",")(0), x.split(",")(1).toInt))
rddPair.collect // les clefs en string et la valeur en integer

val rddgbk = rddPair.groupByKey()
rddbgk.collect

val rdd = sc.parallelize(List("0,12", "1,12", "0,4", "2,11", "1,1", "5,4"))
val rddPair = rdd.map(x => (x.split(",")(0), x.split(",")(1).toInt))
val rbkRDD = rddPair.reduceByKey((x,y) => x+y) // additionne les valeurs par clef

val squareValueRDD = rddPair.mapValues(x => x*x)// s'effectue que sur les valeurs, chaque clef étant traitée de façon unique

rddPair.keys.collect
rddPair.values.collect

val sortByKeyRDD = rddPair.sortByKey() // Pour un descending order: sortByKey(false)


// 6.6 Join, rightOuterJoin, leftOuterJoin et cogroup

val rdd1 = sc.parallelize(List("a,1", "b,0", "c,7").map(x => (x.split(",")(0), (x.split(",")(1).toInt))))
val rdd2 = sc.parallelize(List("d,6", "e,1", "a,8").map(x => (x.split(",")(0), (x.split(",")(1).toInt))))

val joinRDD = rdd1.join(rdd2)
joinRDD.collect // inner join avec (a, (1,8))

val rojRDD = rdd1.rightOuterJoin(rdd2)

val cgRDD = rdd1.cogroup(rdd2) // full outer join

// 6.7 Exercice

val rdd = sc.textFile("/Users/julide/Desktop/projets python/spark/ressources/Temperature_1950.txt")

val rdd1 = rdd.filter(x => x.contains("MAX"))

val rddtuple = rdd1.map(x => (x.split(",")(0), x.split(",")(3).toInt))

val solutionMax = rddtuple.reduceByKey((x,y) => if (x > y) x else y)
// ou
val solutionMax = rddTuple.reduceByKey((x,y) => math.max(x,y))












