package sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkSqlVersion1 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSqlVersion1").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)  //sparksql增强
    val lines = sc.textFile("./data/student")
    val rowRDD: RDD[Row] = lines.map(line => {
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val score = fields(3).toDouble
      Row(id, name, age, score)
    })
    val sch: StructType = StructType(List(//结果类型，其实就是表头，用于描述DataFrame
      StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("score", DoubleType, true)
    ))
    val bdf: DataFrame = sqlContext.createDataFrame(rowRDD, sch)  //创建数据框
    bdf.registerTempTable("t_boy")//把DataFrame先注册临时表
    val result: DataFrame = sqlContext.sql("SELECT * FROM t_boy ORDER BY score desc, age asc")//书写SQL（SQL方法应其实是Transformation）
    result.show()//查看结果（触发Action）
    sc.stop()
  }
}
