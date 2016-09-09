package com.sparkcassa

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._
import org.apache.spark.streaming.{Seconds, StreamingContext}

object CassaStream {

   def main(args: Array[String]) {
  val conf = new SparkConf(true).set("spark.cassandra.connection.host", "localhost")
  val sc = new SparkContext("local","test1",conf)
  val ssc = new StreamingContext(sc,Seconds(20))
  val file = ssc.textFileStream("/home/hduser/Documents/09/")
  val file1 = file.map(line => line.split(','))
  file1.map(line => (line(0).toInt, line(1)))
  .saveToCassandra("cdr_ks", "emp", SomeColumns("emp_id", "emp_name"))
  ssc.start()
  ssc.awaitTermination()
  
  
}
  
}