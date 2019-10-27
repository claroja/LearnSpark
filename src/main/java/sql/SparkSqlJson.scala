package sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlJson {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("SparkSqlJson")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val jsonString = "[{\"id\":1,\"name\":\"小明\"},{\"id\":1,\"name\":\"小明\"}]"
    val ds = spark.createDataset(Seq(jsonString))

    val df: DataFrame = spark.read.json(ds)//指定以后读取json类型的数据(有表头)
    df.show()
    df.write.json("./data/json")

    val df2: DataFrame = spark.read.json("./data/json")//指定以后读取json类型的数据(有表头)
    df2.show()
    spark.stop()
  }
}
