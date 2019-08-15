import org.apache.spark.{SparkConf, SparkContext}

/**
  * @ Author: Mr.Li 
  * @ Date: 2019-08-15 22:12 
  * @ Description: 分组排序；
  *
  **/
object TopNGroup {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("TopNGroup")
    val sc = new SparkContext(sparkConf)
    /**
      * 输入
      * Spark 95
      * Hadoop 68
      * Flink 55
      * Spark 95
      */
    val file = sc.textFile("D:\\SparkExample\\word.txt")
    val rdd = file.map(line=>(line.split(",")(0),line.split(",")(1)))
    // 分组 （A，（1,2）
    val group = rdd.groupByKey()
    val groupsSort = group.map(x=>{
      val key = x._1
      val value = x._2
      val valueSort = value.toList.sortWith(_>_).take(4)
      (key,valueSort)
    })
    // 结果(Hadoop,List(93, 75, 69, 68))
    groupsSort.collect().foreach(println)
    sc.stop()
  }
}
