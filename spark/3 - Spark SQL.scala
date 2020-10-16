// Sommaire

// 7.1 Introduction Spark SQL
// 7.2 Créer un dataframe avec manipulations SQL
// 7.3 Créer une fonction UDF
// 7.4 Dataframe avec un fichier csv - Lecture/ecriture
// 7.5 Dataframe avec un fichier json - Lecture/ecriture
// 7.6 Dataframe avec un fichier parquet - Lecture/ecriture
// 7.7 Dataframe avec postegresql - lecture/ecriture
// 7.8 Creation de dataset à partir d'un dataframe
// 7.9 Exercice


// 7.1 Introduction Spark SQL

val rdd = sc.parallelize(List(25,78,98,86))

val rdd2 = rdd.map(x => (x,1))

val tuple1 = rdd2.reduce((x,y) => (x._1 + y._1, x._2 + y._2))

val mamoyenneDAge = tuple1._1.toFloat / rdd3._2

// limite des RDD
// pas de schéma de données
// pas de moteur d'optimisation

// limites DF
// pas de sécurité de type à la compilation
// pas de conversion objet de domaine

// dataset
// collection distribuée des données
// offre les avantages des RDD
// offre les avantages du moteur d'exécution optimisé de spark SQL
// utilise un encodeur spécialisé pour sérialiser les objets

// 7.2 Créer un dataframe avec manipulations SQL

// à partir d'une collection

import spark.implicits._

val rdd = List(
(1, "Judon", 30)
(64, "Deborah", 46)
)

val maDF = rdd.toDF("Id", "Prenom", "Age")
maDF.printSchema()
maDF.show()

// avec la fonction createDataFrame

import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntergerType}

val sqlContext = new SQLContext(sc)

val rowsRDD = sc.parallelize(Seq(Row("John", 27),
Row("Eric", 25),
Row("Joel", 20)))

val schema = new StructType().
add(StructField("Nom", StructType, true)).
add(StructField("Age", IntegerType, true))

val df = sqlContext.createDataFrame(rowsRdd, schema)

// depuis Spark SQL

// en Spark SQL on crée une table via une vue. Cette vue est directement déclarée sur un DataFrame
// vue temporaire avec createOrReplaceTempView: associée à la session spark en cours
// vue globale createGlobalTempView: associée à une base de données système global_temp
// l'id de la vue est alors préfixée par global_temp

df.createGlobalTempView("equipe")

val dfSql = spark.sql("select * from global_temp.equipe where age > 20")

dfSql.show()

// avec la fonction registerTempTable

df.registerTempTable("equipes")

val dfNBA = sqlContext.sql("select name, age from equipes where age > 20")

dfNBA.show()

// 7.3 Créer une fonction UDF

import org.apache.spark.sql.functions.udf 

val dataf = Seq((0, "hello"), (1, "world")).toDF("id", "text")
val dataf = List((0, "Hello"), (1, "world"))/toDF("id", "text")

dataf.show()

val upperUDF = udf {s: String => s.toUpperCase}
dataf.withColumn("upper", upperUDF(col("text"))).show
dataf.withColumn("upper", upperUDF('text)).show

// 7.4 Dataframe avec un fichier csv - Lecture/ecriture

import org.apache.spark.sql.SQLContext

val sqlContext = new SQLContext(sc)

val maDF = (sqlContext.read
					.format("csv")
					.option("header", "true")
					.option("delimiter", ",")
					.load("/Users/julide/Desktop/projets python/spark/ressources/titanic.csv")
					)

maDF.show()

df.write.csv("un chemin/un nom de fichier")

// 7.5 Dataframe avec un fichier json - Lecture/ecriture

val df = sqlContext.read.json("chemin/fichier.json")

df.printSchema()
df.show()

df.write.json("un chemin/un nom de fichier")

// 7.6 Dataframe avec un fichier parquet - Lecture/ecriture

val parquetDF = spark.read.parquet("chemin/fichier.parquet")

// 7.7 Dataframe avec postegresql - lecture/ecriture

// télécharger le driver au préalable

val jdbcDF = spark.read.format("jdbc").option("url", "jdbc:postgresql://127.0.0.1/postgres")
										.option("dbtable", "public.client").option("user", "postgres")
										.option("password", "azerty123")
										.load()


maDF.write.mode(org.apache.spark.sql.SaveMode.Append).format("jdbc").option("url", "jdbc:postgresql://127.0.0.1/postgres")
										.option("dbtable", "public.client").option("user", "postgres")
										.option("password", "azerty123")
										.save()


// 7.8 Creation de dataset à partir d'un dataframe

val rdd = sc.parallelize(List(
								(1, "Spark"),
								(2, "Databricks")
							)
						)

val df = rdd.toDF("Id", "Name")

val dataset = df.as[(Int, String)] //les datasets sont obligatoirement typés

ds.show

// convertir à partir d'une case class

case class Company(name: String, foundingYear: Int, numEmployees: Int)

val inputSeq = List(
Company("ABC",  1998, 310)
)

val df = sc.parallelize(inputSeq).toDF()

val companyDS = df.as[Company]

// 7.9 Exercice

val df = List(
	("Abdul-jabbar", 1560, "Kareem"),
	("Malone", 1072, "Karl")
).toDF("nom", "points", "prenoms")

df.show

val udf_len10 = udf {x: String => (x.lenght() > 10)}

df.withColumn("lensup10", udf_len10(col("nom"))).show

def islensup10(x: String): Boolean = (x.lenght() > 10)

val udf_len10 = udf {x: String => (islensup10(x)}




