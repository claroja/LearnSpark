package rdd

import java.sql.DriverManager

import org.apache.spark.rdd.{JdbcRDD, RDD}
import org.apache.spark.{SparkConf, SparkContext}
object SparkRddJdbc {
  val getConn = () => {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "root", "111111")
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkRddJdbc").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val jdbcRDD: RDD[(Int, String, Int)] = new JdbcRDD( //这个RDD从数据库里读取数据
      sc,
      getConn,
      "SELECT * FROM logs WHERE id >= ? AND id <= ?", //每个RDD都会执行分割后相同的sql语句,要用全闭区间,不然分割时丢数据
      1,
      5,
      2, //分区数量
      rs => {
        val id = rs.getInt(1)
        val name = rs.getString(2)
        val age = rs.getInt(3)
        (id, name, age)
      }
    )
    val r = jdbcRDD.collect()
    println(r.toBuffer)
    sc.stop()
  }
}