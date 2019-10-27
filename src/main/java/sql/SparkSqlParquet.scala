package sql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSqlParquet {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("SparkSqlParquet")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val lines: Dataset[String] = spark.createDataset(List("1,wang,eng", "2,wei,chi", "3,zhao,math"))//一列的datset
    val df: DataFrame = lines.map(line => {//三列的dataset
    val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val nationCode = fields(2)
      (id, name, nationCode)
    }).toDF("id", "name", "nation") //转换为dataframe
    df.show()

    df.write.parquet("./data/parquet")
    //指定以后读取json类型的数据
    val parquetLine: DataFrame = spark.read.parquet("./data/parquet")
    //val parquetLine: DataFrame = spark.read.format("parquet").load("./data/pq")
    parquetLine.printSchema()
    //show是Action
    parquetLine.show()
    spark.stop()
  }
}
