package rdd
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkRddGroupCheckpoint {
  def main(args: Array[String]): Unit = {
    val topN = 3
    val subjects = Array("python", "java", "php")
    val conf = new SparkConf().setAppName("SparkRddTopNCheckpoint").setMaster("local[4]")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("./data/ck")
    val lines: RDD[String] = sc.textFile("./data/teacher")
    val sbjectTeacherAndOne: RDD[((String, String), Int)] = lines.map(line => {
      val subject = line.split("/")(2).replace(".cn","")
      val teacher = line.split("/")(3)
      ((subject, teacher), 1)
    })
    val reduced: RDD[((String, String), Int)] = sbjectTeacherAndOne.reduceByKey(_+_)
    //val cached = reduced.cache()//cache到内存(标记为Cache的RDD以后被反复使用，才使用cache)
    reduced.checkpoint()
    //scala的集合排序是在内存中进行的，但是内存有可能不够用，可以调用RDD的sortby方法，内存+磁盘进行排序
    for (sb <- subjects) {
      val filtered: RDD[((String, String), Int)] = reduced.filter(_._1._1 == sb)//过滤一个学科的数据
      val favTeacher = filtered.sortBy(_._2, false).take(topN)//调用RDD的sortBy方法，(take是一个action，会触发任务提交)
      println(favTeacher.toBuffer)
    }
    sc.stop()
  }
}
