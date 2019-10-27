package rdd

import java.net.InetAddress
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

//object Rules extends Serializable {
//  val rulesMap = Map("hadoop" -> 2.7, "spark" -> 2.2)
//  //val hostname = InetAddress.getLocalHost.getHostName
//  //println(hostname + "！")
//}
//第三种方式，希望Rules在EXecutor中被初始化（不走网络了，就不必实现序列化接口）
object Rules {
  val rulesMap = Map("hadoop" -> 2.7, "spark" -> 2.2)
  val hostname = InetAddress.getLocalHost.getHostName
  println(hostname + "！")
}

object SerTest {
  def main(args: Array[String]): Unit = {
//    val rules = new Rules //初始化class（在Driver端）,是在一个task中实例化一个对象，多行共用一个
    //var rules = Rules//初始化object（在Driver端）,是在一个executor中实例化一个对象,多个task共用一个
    //println("##" + rules.toString + "##")
    val conf = new SparkConf().setAppName("SerTest").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("./data/ser")
    val r = lines.map(word => {
      //val rules = new Rules//在map的函数中，初始化class,创建一个rules实例,是在每个task中助理每一行时实例化一个对象，浪费资源
      val hostname = InetAddress.getLocalHost.getHostName
      val threadName = Thread.currentThread().getName
      (hostname, threadName, Rules.rulesMap.getOrElse(word, 0), Rules.toString)//rules的实际是在Executor中使用的
    })
    println(r.collect().toBuffer)
//    r.saveAsTextFile("D:\\code\\ip\\serOut.txt")
    sc.stop()
  }
}






