// Les RDDs

// 6.2 Apprendre les bases de la programmation des RDDs
// 6.3 map, flatMap, filter, distinct, groupBy et sample
// 6.4 Union, intersection, substract, cartesian, reduce et fold



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

// 6.4 Union, intersection, substract, cartesian, reduce et fold



