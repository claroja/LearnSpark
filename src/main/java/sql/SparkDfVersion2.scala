package sql

import org.apache.spark.sql.{Dataset, SparkSession}

object SparkDfVersion2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkDfVersion2")
      .master("local[*]")
      .getOrCreate()

    val lines: Dataset[String] = spark.read.textFile("./data/words")
    import spark.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(" "))
//    val r = words.groupBy($"value" as "word").count().count//使用DataSet的API（DSL）
    //导入聚合函数
    import org.apache.spark.sql.functions._
    val counts = words.groupBy($"value".as("word")).agg(count("*") as "counts").orderBy($"counts" desc)
    counts.show()
//    println(r)
    spark.stop()
  }
}
