package cn.edu360.utils

import cn.edu360.demo.Constant
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import redis.clients.jedis.Jedis

object CalculateUtil {
  def calculateIncome(fields: RDD[Array[String]]) = {
    //将数据计算后写入Redis
    val priceRDD: RDD[Double] = fields.map(arr => {
      val price = arr(4).toDouble
      price
    })
    //reduce是一个Action会把结果返回给Driver端
    //将当前批次的总金额返回
    val sum = priceRDD.reduce(_+_)

    //获取一个jedis连接
    val conn: Jedis = JedisConnectionPool.getConnection()


    //将历史值和当前的进行累加
//    conn.set(Constant.TOTAL_INCOME,sum.toString)
    conn.incrByFloat(Constant.TOTAL_INCOME,sum)

    //释放连接
    conn.close()


  }

  /**
    * 计算分类的成交金额
    * @param fields
    */
  def calculateItem(fields: RDD[Array[String]]) = {


    val itemAndPrice: RDD[(String, Double)] = fields.map(arr => {
      //分类
      val item = arr(2)
      //金额
      val price = arr(4).toDouble

      (item, price)
    })
    //按照商品分类进行聚合
    val reduced: RDD[(String, Double)] = itemAndPrice.reduceByKey(_+_)

    //将当前批次数据累加到Redis数据库中
    //foreachPartition是一个Action
    reduced.foreachPartition(part => {
      //获取一个Jedis连接(在每一个executor端获取conn)
      val conn = JedisConnectionPool.getConnection()
      part.foreach(t => {
        //一个连接更新多条数据
        conn.incrByFloat(t._1,t._2)
      })
      //当前分区数据更新完成，连接池关闭
      conn.close()
    })


  }

  def calculateZone(fields: RDD[Array[String]],broadcastRef:Broadcast[Array[(Long, Long, String)]]) = {
    val provinceAndPrice: RDD[(String, Double)] = fields.map(arr => {
      val ip = arr(1)
      val price = arr(4).toDouble
      val ipNum = MyUtils.ip2Long(ip)

      //在Executor中获取到广播的全部规则
      val allRules: Array[(Long, Long, String)] = broadcastRef.value

      //二分法查找
      val index = MyUtils.binarySearch(allRules, ipNum)

      var province = "未知"
      if (index != -1) {
        province = allRules(index)._3
      }
      //返回省份，订单金额
      (province, price)

    })
    //按省份进行聚合
    val reduced: RDD[(String, Double)] = provinceAndPrice.reduceByKey(_+_)
    reduced.foreachPartition(part => {
      //获取jedis连接
      val conn = JedisConnectionPool.getConnection()
      //将数据更新到redis
      part.foreach(t =>{
        conn.incrByFloat(t._1,t._2)
      })
      //关闭Redis连接
      conn.close()
    })
  }
}
