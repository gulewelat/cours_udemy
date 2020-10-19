// Sommaire

// 3.1 Mise en pratique:  Wordcount en streaming
// 3.2 Mise en pratique: Sliding window
// 3.3 Exercice


// 3.1 Mise en pratique:  Wordcount en streaming

val context = new StreamingContext(conf, Seconds(1))

val lines = context.socketTextStream(...)

val words = lines.flatMap(_.split(" "))

val wordCounts = words.map(x => (x,1).reduceByKey(_+_))

wordCounts.print()

context.start()


import org.apache.spark._
import org.apache.spark.streaming._

// Créer un StreamingContext local avec deux threads et un intervalle
// de traitement par lots d'une seconde
// Création du Master avec 2 coeurs
val conf = new SparkConf().setMaster("local[2]")
.setAppName("NetworkCount")
.set("spark.driver.allowMultipleContexts", "true")

// un DStream contient 1 seconde de données
val ssc = new StreamingContext(conf, Seconds(1))

// Spark Streaming se met en attente de données
val lines = ssc.socketTextStream("localhost", 5555)

// wordCounts
val words = lines.flatMap(_.split(" "))
val pairs = words.map(word => (word,1))
val wordCounts = pairs.reduceByKey(_+_)

// Affiche les 10 premiers éléments de chaque RDD généré dans ce DStream sur la console
wordCounts.print()

//lance le calcul
ssc.start()

// Attend la fin du calcul
ssc.awaitTermination()


// 3.2 Mise en pratique: Sliding window

import org.apache.spark._
import org.apache.spark.streaming._

val conf = new SparkConf().setMaster("local[2]")
.setAppName("NetworkCount")
.set("spark.driver.allowMultipleContexts", "true")

val ssc = new StreamingContext(conf, Seconds(2))

val mystream = ssc.socketTextStream("localhost", 5555)

val words = mystream.flatMap(_.split(" ")).map(x => (x,1))

val wordCounts = words.reduceByKeyAndWindow((x:Int, y:Int) => x+y, Seconds(50), Seconds(4))

wordCounts.print()

ssc.start()

// 3.3 Exercice

import org.apache.spark._
import org.apache.spark.streaming._

val conf = new SparkConf().setMaster("local[2]")
.setAppName("NetworkCount")
.set("spark.driver.allowMultipleContexts", "true")

val ssc = new StreamingContext(conf, Seconds(2))

val file = ssc.textFileStream("un chemin")

file.foreachRDD( t =>  {
	val test = t.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_)
	test.saveAsTextFile("un chemin")
})

ssc.start()

// ou

file.foreachRDD( t =>  {
	val test = t.flatMap(line => line.split(" ")).map(word => (word,1)).reduceByKey(_+_)
	if (test.take(1).lenght!= 0){
	test.saveAsTextFile("un chemin"+java.time.LocalDateTime.now.toString)
}})

ssc.start()

