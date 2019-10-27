package rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRddGroup{
  def main(args: Array[String]): Unit = {
    val topN = 3
    val conf = new SparkConf().setAppName("SparkRddTopN").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("./data/teacher")
    val sbjectTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val subject = line.split("/")(2).replace(".cn","")
      val teacher = line.split("/")(3)
      ((subject, teacher), 1)
    })
    val reduced: RDD[((String, String), Int)] = sbjectTeacherAndOne.reduceByKey(_+_)//聚合，将学科和老师联合当做key
    val grouped: RDD[(String, Iterable[((String, String), Int)])] = reduced.groupBy(_._1._1)//按学科进行分组
    //scala的集合排序是在内存中进行的，但是内存有可能不够用，可以替换使用RDD的sorted
    val sorted = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(topN))//经过分组后，一个分区内可能有多个学科的数据，每台机器上都计算一个学科的数据所以可以调用scala的方法,tolist既是scala的方法
    val r: Array[(String, List[((String, String), Int)])] = sorted.collect()
    println(r.toBuffer)
    sc.stop()
  }
}
