package rdd

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRddBroadcast {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkRddBroadcast").setMaster("local[4]")
    val sc = new SparkContext(conf)
    //在Driver端获取IP规则数据,(startNum, endNum, province)
    val rules: Array[(Long, Long, String)] = MyUtils.readRules("./data/ip")
    //将Drive端的数据广播到Executor中,广播变量的引用（还在Driver端）
    val broadcastRef: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(rules)
    val accessLines: RDD[String] = sc.textFile("./data/access")//创建RDD，读取访问日志
    val func = (line: String) => {//driver定义该函数
      val fields = line.split("[|]")
      val ip = fields(1)
      val ipNum = MyUtils.ip2Long(ip)//将ip转换成十进制
      val rulesInExecutor: Array[(Long, Long, String)] = broadcastRef.value //Executor中调用,拿到广播规则
      var province = "未知"
      val index = MyUtils.binarySearch(rulesInExecutor, ipNum)
      if (index != -1) {
        province = rulesInExecutor(index)._3
      }
      (province, 1)
    }
    val proviceAndOne: RDD[(String, Int)] = accessLines.map(func)
    val reduced: RDD[(String, Int)] = proviceAndOne.reduceByKey(_+_)
//    reduced.foreachPartition(it => MyUtils.data2MySQL(it)) //写入数据库
    val r = reduced.collect()
    println(r.toBuffer)
    sc.stop()
  }
}
