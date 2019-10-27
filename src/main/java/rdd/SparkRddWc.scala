package rdd
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object SparkRddWc {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkRddWc").setMaster("local[*]")//创建配置文件
    val sc = new SparkContext(conf) //spark主程序
    val lines: RDD[String] = sc.textFile("./data/words")  // 读取文件
    val words: RDD[String] = lines.flatMap(_.split(" "))//切分压平
    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))//将单词和一组合
    val reduced:RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)//按key进行聚合
//    reduced.saveAsTextFile("./data/wordsOut")//将结果保存到HDFS中,在win下要先配置hdfs
    println(reduced.collect().toBuffer)  //打印
    sc.stop()//释放资源
  }
}
