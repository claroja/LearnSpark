package rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRddTopn {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FavTeacher").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("./data/teacher")
    val teacherAndOne = lines.map(line => {
      val teacher = line.split("/").last //该句不是map的返回
      (teacher, 1) //map方法最后一句话是返回
    })
    val reduced: RDD[(String, Int)] = teacherAndOne.reduceByKey(_+_)//聚合
    val sorted: RDD[(String, Int)] = reduced.sortBy(_._2, false)//降序
    val reslut: Array[(String, Int)] = sorted.collect()//触发Action执行计算
    println(reslut.toBuffer)//打印
    sc.stop()
  }
}
