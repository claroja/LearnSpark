package sql

import rdd.MyUtils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSqlJoinIpLocation {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkSqlJoinIpLocation")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    //创建ip规则数据框
    val rulesLines:Dataset[String] = spark.read.textFile("./data/ip")
    val ruleDataFrame: DataFrame = rulesLines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    }).toDF("snum", "enum", "province")

    //创建访问日志数据框
    val accessLines: Dataset[String] = spark.read.textFile("D:\\code\\ip\\access.log")
    val ipDataFrame: DataFrame = accessLines.map(log => {
      val fields = log.split("[|]")
      val ip = fields(1)
      val ipNum = MyUtils.ip2Long(ip)
      ipNum
    }).toDF("ip_num")

    ruleDataFrame.createTempView("v_rules")
    ipDataFrame.createTempView("v_ips")
    //这样join的代价非常大，executor每遇到一条ip都会拉取一次全量的ip规则
    val r = spark.sql("SELECT province, count(*) counts FROM v_ips JOIN v_rules ON (ip_num >= snum AND ip_num <= enum) GROUP BY province ORDER BY counts DESC")
    r.show()
    spark.stop()
  }
}
