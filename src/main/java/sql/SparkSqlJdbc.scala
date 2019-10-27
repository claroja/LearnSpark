package sql

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSqlJdbc {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("SparkSqlJdbc")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val logs: DataFrame = spark.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://localhost:3306/bigdata",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "logs",
        "user" -> "root",
        "password" -> "123568")
    ).load()

    //logs.printSchema()
    //logs.show()

    //1过滤数据，方法1
//    val filtered: Dataset[Row] = logs.filter(r => {
//      r.getAs[Int]("age") <= 13
//    })
//    filtered.show()

    //2过滤数据，方法2 lambda表达式
    val r = logs.filter($"age" <= 13)
    //val r = logs.where($"age" <= 13)

    val reslut: DataFrame = r.select($"id", $"name", $"age" * 10 as "age")

    val props = new Properties()
    props.put("user","root")
    props.put("password","123568")
    reslut.write.mode("ignore").jdbc("jdbc:mysql://localhost:3306/bigdata", "logs1", props)
    spark.close()
  }
}
