package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkDfVerson1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkDfVerson1").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    val lines = sc.textFile("./data/student")

    val rowRDD: RDD[Row] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val score = fields(3).toDouble
      Row(id, name, age, score)
    })

    val sch: StructType = StructType(List(
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("score", DoubleType, true)
    ))

    val bdf: DataFrame = sqlContext.createDataFrame(rowRDD, sch)
    val df1: DataFrame = bdf.select("name", "age", "score")
    import sqlContext.implicits._ //使用dataframe的API
    val df2: DataFrame = df1.orderBy($"score" desc, $"age" asc)
    df2.show()
    sc.stop()
  }
}
