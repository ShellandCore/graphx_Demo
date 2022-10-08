package cn.kgc.SparkGraphx_Flight

import org.apache.spark.SparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SparkSession}


object Flight {
  def main(args: Array[String]): Unit = {
    //    创建SparkSession
    val spark: SparkSession = SparkSession.builder()
      .appName(this.getClass.getName)
      .master("local[4]")
      .getOrCreate()
    //    创建SparkContext
    val sc: SparkContext = spark.sparkContext

    val lines: RDD[String] = sc
      .textFile("data/Flight/USA Flight Datset - Spark Tutorial - Edureka.csv")


    //TODO：方式一：mapPartitionsWithIndex通过分区标号去首行
    val textRDD: RDD[String] = lines.mapPartitionsWithIndex((index, iter) => {
      //传入参数：(index, iter):index是分区号，iter是Iterator[T]：分区中的元素
      //传出：Iterator[T]：分区中的元素
      //删除分区为0的第一行数据
      if (index == 0) iter.drop(1)
      else iter
    })
    val rdd: RDD[(String, String, String, String, String)] = textRDD.map(x => {
      x.split(",")
    }).map(x => {
      //关键字段：起飞机场编号、起飞机场、到达机场编号、到达机场、飞行距离

      (x(5), x(6), x(7), x(8), x(16))
    })
    //   rdd.take(10).foreach(println)
    //TODO:构建点集合
    val vertRDD: RDD[(Long, String)] = rdd.flatMap(x => Array(
      (x._1.toLong, x._2.toString),
      (x._3.toLong, x._4.toString)
    )).distinct()
    //TODO:构建边集合
    val edgesRDD: RDD[Edge[String]] = rdd.map(x =>
      Edge(x._1.toLong, x._3.toLong, x._5)
    ).distinct()
    //TODO:构建图
    val graph = Graph(vertRDD, edgesRDD, " unknow") //增加默认值


    //TODO：1.机场数量
    println()
    println("机场数量：" + graph.numVertices)
    //TODO:2.航线数量
    print("航线数量：")
    println(graph.numEdges)
    //TODO:3.最大的边属性(最长的飞行距离)
    //对triplets按飞行距离排序（降序）并取第一个
    print("最长的飞行距离:")
    println(graph.triplets.sortBy(_.attr, ascending = false).first())
    //TODO:4.哪个机场到达航班最多
    //计算顶点的入度并排序
    print("到达航班最多的机场：")
    println(graph.inDegrees.repartition(1)
      .sortBy(_._2, false).first())
    //TODO:5.找出最重要的飞行航线
    //   PageRank收敛误差：0.05
    val importantFlightGraph: Graph[Double, Double] = graph.pageRank(0.05)
    print("最重要的飞行航线：")
    //takeOrdered(3)(Ordering.by(_.attr))：
    //按照-_.attr进行降序排序，拿出前三个
    importantFlightGraph.edges.takeOrdered(3)(Ordering.by(-_.attr)).foreach(println)
    println("------------")
    println(importantFlightGraph.edges.repartition(1)
      .sortBy(_.attr, false)
      .first())
    //TODO:6.找出最便宜的飞行航线
    //1.定价模型
    //price = 180.0 + distance * 0.15
    //2.SSSP问题
    //从初始指定的源点到达任意点的最短距离
    //3.pregel
    //初始化源点（0）与其它顶点（Double.PositiveInfinity）
    //初始消息（Double.PositiveInfinity）
    //vprog函数计算最小值
    //sendMsg函数计算进行是否下一个迭代
    //mergeMsg函数合并接受的消息，取最小值

    //todo:
    println("最便宜的飞行航线:")
    // todo:将边的属性变成价格,赋给源点(12478)一个初值，0.0，其他点无穷大
    val graph2: Graph[Double, Double] = graph.mapEdges(e => 180.0 + e.attr.toInt * 0.15)
      .mapVertices((id, attr) => if (id == 12478) 0.0 else Double.PositiveInfinity)
    //TODO:调用pregel
    graph2.pregel(Double.PositiveInfinity)(//初始值，无穷大
        // todo:①vprog：接收消息的处理函数
        (id, dist, new_dist) => math.min(dist, new_dist),
        // todo:②sendMsg：发送消息的处理函数
        triplet => {
          //如果当前点的价格+边长的价格<目标点已有的价格就发送消息
          //否则就不发送消息
          if (triplet.srcAttr + triplet.attr < triplet.dstAttr) Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))
          else Iterator.empty
        },
      //todo:③mergeMsg:
        (a, b) => math.min(a, b)
      ).vertices.takeOrdered(3)(Ordering.by(_._2)).foreach(println)


  }
}
// //输出
// 机场数量：301
// 航线数量：4088
// 最长的飞行距离:((11995,GSO),(11298,DFW),999)
// 到达航班最多的机场：(10397,152)
// 最重要的飞行航线：Edge(10141,13487,1.0)
// Edge(10208,10397,1.0)
// Edge(10170,10299,1.0)
// ------------
// Edge(10146,10397,1.0)
// 最便宜的飞行航线:
// (12478,0.0)
// (10821,207.6)
// (10721,208.05)
