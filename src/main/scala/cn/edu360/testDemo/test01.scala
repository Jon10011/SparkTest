package cn.edu360.testDemo

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.SortedMap

object test01 {
  /**
    * 求最大值，最小值，平均值
    */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("test01").setMaster("local[*]")
    val sc = new SparkContext(conf)

    //    topN的值
    val topNum = 5

    val topN: Broadcast[Int] = sc.broadcast(topNum)

    val lines = sc.textFile("/Users/jon/Desktop/20190820logs.txt").filter(_.trim().length() > 0)

    val words1: RDD[(String, Int)] = lines.map(line => ("key", line.replace("-#", "").split("#")(4).toInt))

    val words2: RDD[(String, Int)] = lines.map(line => (line.replace("-#", "").split("#")(3), 1))

    val words3: RDD[(String, Int)] = lines.map(line => (line.replace("-#", "").split("#")(2), 1))

    words2.groupByKey().map(x => {
      var total = 0
      for (a <- x._2) {
        total = total + 1
      }
      (x._1, total)
    }).collect.foreach(x => {
      println("响应状态：" + x._1 + "的数量为" + x._2)
    })

    //    方法1
    words1.groupByKey().map(x => {
      var min = Integer.MAX_VALUE
      var max = Integer.MIN_VALUE
      var sum = 0
      var store = 0.0

      for (num <- x._2) {
        store = store + 1
        sum = sum + num
        if (num > max) {
          max = num

        }
        if (num < min) {
          min = num
        }
      }
      val avg = sum / store
      val fm = f"$avg%1.2f" //1.2->取后面两位小数,格式化数据
      (max, min, fm)
    }).collect.foreach(x => {
      println("最大值是\t" + x._1)
      println("最小值是\t" + x._2)
      println("平均值是\t" + x._3)
    })


    //    方法2

    words1.groupByKey().map({ x =>
      println("最大值是\t" + x._2.max)
      println("最小值是\t" + x._2.min)
    }).collect()
    //聚合
    val reduced: RDD[(String, Int)] = words3.reduceByKey(_ + _)

    //    //分组排序
    //    val grouped: RDD[(String, Iterable[(String, Int)])] = reducesd.groupBy(_._1)
    //
    //    //将每一个组拿出来进行操作
    //    val sorted: RDD[(String, List[(String, Int)])] = grouped.mapValues(_.toList.sortBy(_._2).reverse.take(5))
    //
    //
    //    //收集结果
    //    val r: Array[(String, List[(String, Int)])] = sorted.collect()
    //
    //    //打印
    //    println(r.toBuffer)
    //    sc.stop()

//    val kv: RDD[(String, Int)] = words3.groupByKey().map(x => {
//      var total = 0
//      for (a <- x._2) {
//        total = total + 1
//      }
//      (x._1, total)
//    })
    //求topN
    //分析：非唯一key
    //将非唯一key转化为唯一key
    val uniqueKey: RDD[(String, Int)] = reduced.reduceByKey(_ + _)
    val partitions: RDD[(Int, String)] = uniqueKey.mapPartitions(itr => {
      var sortedMap: SortedMap[Int, String] = SortedMap.empty[Int, String]
      itr.foreach { tuple => {
        sortedMap += tuple.swap
        if (sortedMap.size > topN.value) {
          sortedMap = sortedMap.takeRight(topN.value)
        }
      }
      }
      sortedMap.takeRight(topN.value).toIterator
    })

    val alltopN = partitions.collect()
    alltopN.foreach(a => println(a))

    val finaltopN = SortedMap.empty[Int, String].++:(alltopN)
    val resultUsingMapPartition = finaltopN.takeRight(topN.value)
    //打印结果
    println("+---+---+ TopN的结果1：")
    resultUsingMapPartition.foreach {
      case (k, v) => println(s"$v \t $k")
    }

    val createCombiner: Int => Int = (v: Int) => v
    val mergeValue: (Int, Int) => Int = (a: Int, b: Int) => a + b
    val moreConciseApproach: Array[(Int, Iterable[String])] =
      reduced.combineByKey(createCombiner, mergeValue, mergeValue)
        .map(_.swap)
        .groupByKey()
        .sortByKey(ascending = false)
        .take(topN.value)

    //打印结果
    println("+---+---+ TopN的结果2：")
    moreConciseApproach.foreach {
      case (k, v) => println(s"${v.mkString("")} \t $k")
    }
  }
}
