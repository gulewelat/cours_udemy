// cours spark avec scala

// Section 5: langage scala

// Sommaire

//  5.1 Variable et opérations
//  5.2 Boucles, procédures et fonctions
// 5.3 Curriying, tableaux et les lists
// 5.4 Pattern matching



//  5.1 Variable et opérations

46

46.toString()

46 == 47

46 != 47

46.abs

46.toHexString

46.isValidInt

var mutable = 21

mutable = mutable + 1

val immutable = 21 

immutable = immutable + 1

val immutable2 = immutable + 1


//  5.2 Boucles, procédures et fonctions

var i : Int = 0

while (i < 10) {
	println(i)
	i = i+1 }


for (i <- 0 to 9)
println(i)

//  fonctions

def max(x: Int, y: Int) = if(x > y) x else y

def opp(x: Int) : Int = -x
def opp(x: Int) : Int = { return -x; }

//  fonctions anonymes

val liste = List( "bleu", "rouge", "vert", "blanc")

liste.filter(s => s.startsWith("b"))

val cnt = liste.count(s => s.startsWith("b"))

print(res7)

print(cnt)

// syntaxe procédurale

def sayHello (aQui: String){
	println(s"hello $aQui")
} // prête à confusion


def sayHello (aQui: String) : Unit = {
	println(s"hello $aQui")
} // vaut mieux écrire de cette façon avec Unit


def sayHello (aQui: String) : Unit = {
	println(s"hello" +aQui)
}

// 5.3 Curriying, tableaux et les lists

def divs(m : Int)(n : Int) = (n % m == 0)

val param1 = divs(2)_

println(param1(4))

val a = new Array[String](2) // On initialise un tableau

a(0) = "Java"
a(1) = "rocks"

a(0) = "Scala" // on modifie le premier élément d'un tableau

a // afficher tableau

for (i <- 0 to 1) println(a(i)) // println au lieu de. print pour que le résultat ne soit pas concaténé

val list1 = List(1, 2, 3)

val list2 = List("Thomas", "Young", 56)

val list1 = 1 :: 2 :: 3 :: Nil // Nil pour finir, on ne passe pas par la méthode List

list1.filter( _ > 1)

list1(0)

list1.filter(x => x > 1)

val list2 = list1 ::: 4 :: Nil // attention les listes dans scala sont immuables, on peut cependant ajouter des éléments à la fin 

val list3 = 0 :: list2

val list4 = list3 :: 0 :: Nil


// 5.3 Les collections: set, tuple et map

// les sets ne sont pas ordonnés, on ne peut pas accéder à leurs élements par position

val set1 = Set(1, 2, 3, 4)

val set2 = Set(4, 5, 6, 7)

val set3 = Set(1, 2, 3, 4, 4)

set1.min

set1.max

set1.intersect(set2)

set1.union(set2)

set1.diff(set2)

// on peut spécifier un set mutable

var mutableset = scala.collection.mutable.Set(1, 2, 3, 4)

mutableset += 5

// deux façon de créer un tuple

val a_tuple = ("Thomas", "Young", 56)

val another_tuple = new Tuple3("Thomas", "Young", 56)

println(a_tuple._1)

val(fname, lname, age) = a_tuple

// map équivalent au dictionnaire en python

val a_map = Map("fname" -> "Thomas", "lname" -> "Young", "age" -> 56)

a_map.keys

a_map.values

// on peut faire des map mutables

var mutablemap = scala.collection.mutable.Map("fname" -> "Thomas", "lname" -> "Young", "age" -> 56)

mutablemap.keys

mutablemap += ("city" -> "London")

mutablemap.keys.foreach(x => println(x))

// Map: classe, map: méthode, faire très attention


// 5.4 Pattern matching

def matchTest(x:Int) : String = x match {

	case 0 => "zero"
	case 1 => "one"
	case 2 => "two"
	case _ => "many"
}

matchTest(3)

// 5.5 Les classes et case class

// Une classe est un type permettant de regrouper dans la même structure: les informations (champs, propriétés, attributs), les procédures et fonctions (méthodes)
// La classe est un type structuré mais va + loin que l'enrengistrement
// Peut faire référence à d'instances à d'autres classes

// Classe est la structure 
// Objet est une instance de la classe (variable obtenue après instanciation)
// Instanciation correspond à la création d'un objet

class Personne(val name: String, var age:Int) {} // une classe simple

var p1 = new Personne("Stephane", 32) // un objet
var p2 = new Personne("Sophie", 15)
p2.age = 15 // accès direct à un membre

p1.name
p2.name
p2.age

class Personne // notre classe est définie

class Point(var x: Int, var y: Int){

	def move(dx: Int, dy: Int): Unit = {
		x = x + dx
		y = y + dy
	}
}

val point1 = new Point(2,3)
point1.x //2
point1.y //3

val origine = new Point // x et y initialisés à 0
val point1 = new Point(5)
println(point1.x) // prints 5

val point2 = new Point(y=2)

val po1 = new Point
po1.x // 0
po1.y // 0

val po2 = new Point(5) // y reste à 0, seul x spécifié

// définir une case class
// on peut définir une case class avec le mot clef "case class", un identifiant plus un paramètre

case class Book(isbn: String)

val id_livre = Book('lskjdhfdsqlsj') 
// pas besoin de mttre "new", les case class ont une méthod apply par défaut qui prend en charge la construction de l'objet

case class Message(expediteur: String, destinataire: String, body: String)
val message1 = Message("thomas@gmail.com", "durand@gmail.com", "Ca va?")
println(message1.expediteur)

message1.expediteur = "fred@gmail.com" // ne compile pas car je n'ai pas appelé var et par défaut val dans une case class

case class Message2(expediteur: String, destinataire: String, body: String)
var message2 = Message("thomas@gmail.com", "durand@gmail.com", "Ca va?")
message2.expediteur = "toto@gmail.com"







