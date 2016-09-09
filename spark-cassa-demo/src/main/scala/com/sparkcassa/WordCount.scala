package com.sparkcassa

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object WordCount {
  
  def main(args: Array[String]) {
  
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val input = sc.textFile("/home/hduser/Documents/wordcount.txt")
    val words = input.flatMap(line => line.split(" "))
    val counts = words.map(word => (word, 1)).reduceByKey{case (x, y) => x + y}
    counts.saveAsTextFile("/home/hduser/Documents/output4")
    //println(counts)
    
}

}