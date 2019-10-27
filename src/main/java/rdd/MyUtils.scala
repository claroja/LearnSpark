package rdd

import java.sql.{Connection, DriverManager, PreparedStatement}

import scala.io.{BufferedSource, Source}

object MyUtils {
  def ip2Long(ip: String): Long = {//将ip地址转换为十进制
    val fragments = ip.split("[.]")
    var ipNum = 0L
    for (i <- 0 until fragments.length){
      ipNum =  fragments(i).toLong | ipNum << 8L
    }
    ipNum
  }
  def readRules(path: String): Array[(Long, Long, String)] = {//获取ip范围对应的省份
    val bf: BufferedSource = Source.fromFile(path)
    val lines: Iterator[String] = bf.getLines()
    val rules: Array[(Long, Long, String)] = lines.map(line => {
      val fileds = line.split("[|]")
      val startNum = fileds(2).toLong  //ip地址的起始
      val endNum = fileds(3).toLong  //ip地址的终止
      val province = fileds(6)  //获得省份信息
      (startNum, endNum, province)
    }).toArray
    rules
  }
  def binarySearch(lines: Array[(Long, Long, String)], ip: Long) : Int = {//根据ip范围进行二分查找
    var low = 0
    var high = lines.length - 1
    while (low <= high) {
      val middle = (low + high) / 2
      if ((ip >= lines(middle)._1) && (ip <= lines(middle)._2))
        return middle
      if (ip < lines(middle)._1)
        high = middle - 1
      else {
        low = middle + 1
      }
    }
    -1
  }

  def data2MySQL(it: Iterator[(String, Int)]): Unit = {//将分区数据存储到mysql
    val conn: Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "root", "aaaaaa")
    val pstm: PreparedStatement = conn.prepareStatement("INSERT INTO access_log VALUES (?, ?)")
    it.foreach(tp => {
      pstm.setString(1, tp._1)
      pstm.setInt(2, tp._2)
      pstm.executeUpdate()
    })
    if(pstm != null) {
      pstm.close()
    }
    if (conn != null) {
      conn.close()
    }
  }


  def main(args: Array[String]): Unit = {//测试方法
    val rules: Array[(Long, Long, String)] = readRules("D:\\code\\ip\\ip.txt")
    val ipNum = ip2Long("115.205.13.42")
    val index = binarySearch(rules, ipNum)
    val tp = rules(index)
    val province = tp._3
    println(province)

  }
}
