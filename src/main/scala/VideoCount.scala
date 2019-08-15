import org.apache.spark.{SparkConf, SparkContext}

/**
  * author: Mr.Li 
  * create: 2019-08-15 12:40 
  * description: 这个是统计视频观看次数的样例；
  * 要统计用户的总访问次数和去除访问同一个URL之后的总访问次数,
  * 随便造了几条样例数据(四个字段:id,name,vtm,url,vtm字段本例没用,不用管)如下:
  * id1,user1,2,http://www.hupu.com
  * id1,user1,2,http://www.hupu.com
  * id1,user1,3,http://www.hupu.com
  * id1,user1,100,http://www.hupu.com
  * id2,user2,2,http://www.hupu.com
  * id2,user2,1,http://www.hupu.com
  * id2,user2,50,http://www.hupu.com
  * id2,user2,2,http://touzhu.hupu.com
  * 如果用sql 写 如下：
  * select id ,name ,count(id) , count(distinct url) from table group by id,name;
  *
  **/
object VideoCount {
  /**
    * 总结一下：用spark的话估计会很容易，但是用rdd确实很繁琐。
    * @param args
    */
  def main(args: Array[String]): Unit = {
    //初始化spark
    val sparkConf = new SparkConf()
      .setMaster("local[2]")
      .setAppName("TrafficModel")
    val sc = new SparkContext(sparkConf)
    val fileRdd = sc.textFile("D:\\SparkExample\\Video.txt")
    // 使用样例类来保存每一行数据
    case class Video(id: String,name:String,vtm:String,url:String)
    // 使用rdd解决一下这个问题；
    val mapRdd = fileRdd.map(line=>{
      val v = line.split(",")
      Video(v(0),v(1),v(2),v(3))
    }).map(x=>{
      // 把他用元组组成
      // ((id1,user1),http://www.hupu.com)
      ((x.id , x.name),x.url)
    })
    // 这个是给访问的记录加上有几条数据
    // 用list来存url的数据
    val seqOp = (a:(Int,List[String]),b:String) => a match {
      case (0,List()) => (1,List(b))
      case _ => (a._1+1,b::a._2)
    }

    // 这个就是合并了
    val combOp = (a:(Int,List[String]),b:(Int,List[String])) =>{
      (a._1+b._1,a._2:::b._2)
    }

    // 初始值为(0,List[String]())
    val aggRdd = mapRdd.aggregateByKey((0,List[String]()))(seqOp,combOp).map(a=>{
      (a._1,a._2._1,a._2._2.distinct.length)
    })
    aggRdd.collect().foreach(println)

    println("==============")
    sc.stop()
  }
}