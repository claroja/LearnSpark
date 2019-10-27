package sql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSqlVersion2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("SparkSqlVersion2")
      .master("local[*]")
      .getOrCreate()

    val lines: Dataset[String] = spark.read.textFile("./data/words")//Dataset分布式数据集，是对RDD的进一步封装，是更加智能的RDD，只有一列，默认这列叫value

    import spark.implicits._
    val words: Dataset[String] = lines.flatMap(_.split(" ")) //由于隐式转换的作用直接可以读成DF
    words.createTempView("v_wc")//注册视图
    val result: DataFrame = spark.sql("SELECT value word, COUNT(*) counts FROM v_wc GROUP BY word ORDER BY counts DESC")
    result.show()
    spark.stop()
  }
}
