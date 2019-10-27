package sql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSqlJoin {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkSqlJoin")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    val lines: Dataset[String] = spark.createDataset(List("1,wang,eng", "2,wei,chi", "3,zhao,math"))//一列的datset
    val tpDs: Dataset[(Long, String, String)] = lines.map(line => {//三列的dataset
      val fields = line.split(",")
      val id = fields(0).toLong
      val name = fields(1)
      val nationCode = fields(2)
      (id, name, nationCode)
    })
    val df1 = tpDs.toDF("id", "name", "nation") //转换为dataframe

    val nations: Dataset[String] = spark.createDataset(List("eng,英语", "chi,汉语"))
    val ndataset: Dataset[(String, String)] = nations.map(l => {
      val fields = l.split(",")
      val ename = fields(0)
      val cname = fields(1)
      (ename, cname)
    })
    val df2 = ndataset.toDF("ename","cname")
    df2.count()
    //第一种，sql
    df1.createTempView("v_users")
    df2.createTempView("v_courses")
    val r: DataFrame = spark.sql("SELECT name, cname FROM v_users JOIN v_courses ON nation = ename")
    //第二种，dsl
//    val r = df1.join(df2, $"nation" === $"ename", "left_outer")
    r.show()
    spark.stop()
  }
}
