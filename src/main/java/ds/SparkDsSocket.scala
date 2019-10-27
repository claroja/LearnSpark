package ds

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object SparkDsSocket {
  def main(args: Array[String]): Unit = {
    //离线任务是创建SparkContext，现在要实现实时计算，用StreamingContext
    val conf = new SparkConf().setAppName("SteamingWordCount").setMaster("local[2]") //必须起两个以上线程，一个用来接收数据，一个用来计算
    val sc = new SparkContext(conf)
    //StreamingContext是对SparkContext的包装，包了一层就增加了实时的功能//第二个参数是小批次产生的时间间隔
    val ssc = new StreamingContext(sc, Milliseconds(5000))//有了StreamingContext，就可以创建SparkStreaming的抽象了DSteam
//    ssc.checkpoint("D:\\code\\ip\\ds") //在有状态下要记录
    //从一个socket端口中读取数据yum install -y nc// nc -lk 9999
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("192.168.1.220", 9999)
    val words: DStream[String] = lines.flatMap(_.split(" "))
    val wordAndOne: DStream[(String, Int)] = words.map((_, 1))
    val reduced: DStream[(String, Int)] = wordAndOne.reduceByKey(_+_)  //只记录当前传输进来的数据
//    val reduced: DStream[(String, Int)] = wordAndOne.updateStateByKey(StatefulKafkaWordCount2.updateFunc, new HashPartitioner(ssc.sparkContext.defaultParallelism), true)//由状态记录数据
    reduced.print()

    ssc.start()
    ssc.awaitTermination()
  }
}


object StatefulKafkaWordCount2 {
  /**
    * 第一个参数：聚合的key，就是单词
    * 第二个参数：当前批次产生批次该单词在每一个分区出现的次数
    * 第三个参数：初始值或累加的中间结果
    */
  val updateFunc = (iter: Iterator[(String, Seq[Int], Option[Int])]) => {
    //iter.map(t => (t._1, t._2.sum + t._3.getOrElse(0)))
    iter.map { case (x, y, z) => (x, y.sum + z.getOrElse(0)) }
  }
}

