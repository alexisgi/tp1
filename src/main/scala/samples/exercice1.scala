package samples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object exercice1 {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    //Question 1 : Lire le fichier "films.csv" sous forme de RDD[String]
    val rdd = sparkSession.sparkContext.textFile("data/donnees.csv")

    //Question 2 : Combien y a-t-il de films de Leonardo Di Caprio dans ce fichier ?
    val acteurRdd = rdd.filter(elem => elem.contains("Di Caprio"))
    println(acteurRdd.count())

    //Question 3 : Quelle est la moyenne des notes des films de Di Caprio ?

    val notes = acteurRdd.map(item => (item.split(";")(2).toDouble))
    val moyenne = notes.sum() / notes.count()
    println(moyenne)

    //Question 4 : Quel est le pourcentage de vues des films de Di Caprio par rapport à l'échantillon que nous avons ?

    val views = acteurRdd.map(item => (item.split(";")(1).toDouble))
    val all_Views = rdd.map(item => (item.split(";")(1).toDouble))
    val rateViewsDiCap = views.sum() / all_Views.sum()
    println(rateViewsDiCap)

    //Question 5 : Quelle est la moyenne des notes par film dans cet échantillon ?
    //    1. Pour cette question, il faut utiliser les Pair-RDD


    //Question 6 : Quelle est la moyenne des vues par film dans cet échantillon ?

  }
}