package sql

import rdd.MyUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object SparkSqljoinIpLocation2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("SparkSqljoinIpLocation2")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    val rulesLines:Dataset[String] = spark.read.textFile("./data/ip")

    //整理ip规则数据()
    val rluesDataset = rulesLines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val province = fields(6)
      (startNum, endNum, province)
    })
    //收集ip规则到Driver端
    val rulesInDriver: Array[(Long, Long, String)] = rluesDataset.collect()
    //将广播变量的引用返回到Driver端(必须使用sparkcontext)
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = spark.sparkContext.broadcast(rulesInDriver)
    //访问日志
    val accessLines: Dataset[String] = spark.read.textFile("./data/access")
    val ipDataFrame: DataFrame = accessLines.map(log => {
      val fields = log.split("[|]")
      val ip = fields(1)
      val ipNum = MyUtils.ip2Long(ip)
      ipNum
    }).toDF("ip_num")
    ipDataFrame.createTempView("v_log")

    //定义一个自定义函数（UDF），并注册（输入一个IP地址对应的十进制，返回一个省份名称）
    spark.udf.register("ip2Province", (ipNum: Long) => {
      val ipRulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value
      val index = MyUtils.binarySearch(ipRulesInExecutor, ipNum)
      var province = "未知"
      if(index != -1) {
        province = ipRulesInExecutor(index)._3
      }
      province
    })
    val r = spark.sql("SELECT ip2Province(ip_num) province, COUNT(*) counts FROM v_log GROUP BY province ORDER BY counts DESC")
    r.show()
    spark.stop()
  }
}
