package rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object SparkRddSort2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkRddSort2").setMaster("local[*]")
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
    val sorted: RDD[(String, Int, Int)] = tpRDD.sortBy(tp => new User(tp._2, tp._3))//sortBy传入的是比较规则，所以返回的不是User类，而是元组
    println(sorted.collect().toBuffer)
    sc.stop()
  }
}

class User(val age: Int, val score: Int) extends Ordered[User] with Serializable {
  override def compare(that: User): Int = {//传入排序规则：年龄升序,分数降序
    if(this.age == that.age) {
      -(this.score - that.score)
    } else {
      this.age - that.age
    }
  }
}


