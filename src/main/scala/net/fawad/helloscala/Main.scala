package net.fawad.helloscala

import org.apache.spark.{SparkConf, SparkContext}

object Main extends App {
  def length(xs:List[Int]):Int = xs match {
    case head::tail  => 1 + length(tail)
    case _ => 0
  }

  val xs = List(1,2,3)
  println(s"Length of ${xs} is ${length(xs)}")

  def reverse(xs:List[Int]):List[Int] = xs match {
    case head::tail => reverse(tail):::List(head)
    case Nil => Nil
  }

  println(s"Reverse of ${xs} is ${reverse(xs)}")

  val sc = new SparkContext(new SparkConf().setMaster("local[4]").setAppName("Hello world"))
  val speeches = sc.textFile("/Users/halimf/Downloads/qconsf2015/data/state-of-union")
  println(speeches.flatMap(_.split("[ \\t\\r\\n]+")).map(x=>(x.toLowerCase, 1)).reduceByKey(_+_).takeOrdered(50)(Ordering[Int].reverse.on{case(_,v)=>v}).map{case(k,_)=>k})

  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val movies = sqlContext.read.json("/Users/halimf/Downloads/qconsf2015/data/movies-json")
  movies.printSchema
  movies.registerTempTable("movies")
  sqlContext.sql("SELECT year, COUNT(*) c FROM movies GROUP BY year ORDER BY c DESC").show
}
