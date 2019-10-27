package rdd
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import scala.collection.mutable

object SparkRddGroupParitioner {
  def main(args: Array[String]): Unit = {
    val topN = 3
    val conf = new SparkConf().setAppName("SparkRddTopNParitioner").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("./data/teacher")
    val sbjectTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val subject = line.split("/")(2).replace(".cn","")
      val teacher = line.split("/")(3)
      ((subject, teacher), 1)
    })
    val reduced: RDD[((String, String), Int)] = sbjectTeacherAndOne.reduceByKey(_+_)
    val subjects: Array[String] = reduced.map(_._1._1).distinct().collect()
    val sbPatitioner = new SubjectParitioner(subjects);//自定义一个分区器，并且按照指定的分区器进行分区
    val partitioned: RDD[((String, String), Int)] = reduced.partitionBy(sbPatitioner)//partitionBy按照指定的分区规则进行分区
    val sorted: RDD[((String, String), Int)] = partitioned.mapPartitions(it => {
      it.toList.sortBy(_._2).reverse.take(topN).iterator//将迭代器转换成list，然后排序，在转换成迭代器返回
    })
    val r: Array[((String, String), Int)] = sorted.collect()
    println(r.toBuffer)
    sc.stop()
  }
}

//自定义分区器
class SubjectParitioner(sbs: Array[String]) extends Partitioner {
  val rules = new mutable.HashMap[String, Int]()
  var i = 0
  for(sb <- sbs) {
    //rules(sb) = i
    rules.put(sb, i)
    i += 1
  }
  override def numPartitions: Int = sbs.length//返回分区的数量（下一个RDD有多少分区）
  //根据传入的key计算分区标号
  //key是一个元组（String， String）
  override def getPartition(key: Any): Int = {
    val subject = key.asInstanceOf[(String, String)]._1//获取学科名称
    rules(subject)//根据规则计算分区编号
  }
}
