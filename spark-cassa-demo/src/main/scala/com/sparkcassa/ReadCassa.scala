package com.sparkcassa

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import com.datastax.spark.connector._

object ReadCassa {

  def main(args: Array[String]) {

    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")
    val sc = new SparkContext("local","test",conf)
    val test_spark_rdd = sc.cassandraTable("cdr_ks", "cdr10")
    println(test_spark_rdd.count())
  
}
}