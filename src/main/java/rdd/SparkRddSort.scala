package rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRddSort {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CustomSort5").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val users= Array("zhao 28 89", "qian 29 99", "sun 22 80", "li 28 99")
    val lines: RDD[String] = sc.parallelize(users)
    val tpRDD: RDD[(String, Int, Int)] = lines.map(line => {
      val fields = line.split(" ")
      val name = fields(0)
      val age = fields(1).toInt
      val score = fields(2).toInt
      (name, age, score)
    })
    val sorted: RDD[(String, Int, Int)] = tpRDD.sortBy(tp => (tp._2,-tp._3))  //传入排序规则：年龄升序,分数降序
    println(sorted.collect().toBuffer)
    sc.stop()
  }
}
