//package com.aura.bigdata.spark.scala.newHdoopapi
//
//import java.text.{DecimalFormat, SimpleDateFormat}
//import java.util
//import java.util.concurrent.{CountDownLatch, Executors, Future}
//
//import ba.common.log.enums.{LogLevel, LogType}
//import ba.common.log.utils.LogUtil
//import cn.piesat.constants.{HbaseZookeeperConstant, RowkeyConstant}
//import cn.piesat.domain._
//import cn.piesat.service.impl.{MsgServiceImpl, SparkTaskServiceImpl}
//import cn.piesat.thread.HbaseQueryThread
//import com.google.gson.Gson
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.client.{Result, Scan}
//import org.apache.hadoop.hbase.filter.{Filter, FilterList}
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//import org.apache.hadoop.hbase.protobuf.ProtobufUtil
//import org.apache.hadoop.hbase.util.{Base64, Bytes}
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.spark.rdd.RDD
//import org.apache.spark.{SparkConf, SparkContext}
//import pie.storage.db.domain._
//import pie.storage.db.enums.{CompareOp, DataBaseType}
//
///**
//  * @author liujie
//  *         spark查询hbase的入口类
//  */
//object HbaseReader {
//  val sparkTaskService = new SparkTaskServiceImpl
//  val msgService = new MsgServiceImpl
//  val sparkAppName = "sparkApp"
//  val sparkMaster = "local[6]"
//  var taskId = 8
//  val serviceNum = 76
//  val systemId = 12011
//  val systemName = "8888"
//  val cf = "cf1"
//  val cell = "content"
//  val zookeeperHost = "bigdata03,bigdata04,bigdata05"
//  val zookeeperPort = "2181"
//  val excutor=Executors.newCachedThreadPool()
//
//  def main(args: Array[String]): Unit = {
//    try{
//      if (args.length > 0) {
//        taskId = args(0).toInt
//      }
//      /**
//        * 第一步，获取SparkContext对象
//        */
//      val sc = getSparkContext
//      /**
//        * 第二步，获得查询参数集合
//        */
//      val taskParamList = getTaskParam(taskId, sc)
//      /**
//        * 第三步，进行hbase数据查询
//        */
//      val rowkeyRDD = queryHbaseData(taskParamList, sc)
//
//      rowkeyRDD.saveAsTextFile("file://")
//      println("rowkeyRDD的数量为：" + rowkeyRDD.count())
//      val rowkey = rowkeyRDD.first()
//      println("取出的值为："+util.Arrays.toString(rowkey._2.getValue(cf.getBytes(),cell.getBytes())))
//
//      /**
//        * 第四步，进行数据解析
//        */
//
//      /**
//        * 第五步，将结果写入文本，文本地址在第二步中的taskParamList中
//        */
//
//    }catch {
//      case e:Exception =>{
//        e.printStackTrace()
//      }
//    }finally {
//      excutor.shutdown()
//    }
//
//
//    excutor.shutdown()
//
//  }
//
//  /**
//    * 获取任务Id
//    *
//    * @param args
//    * @return
//    */
//  private def getTaskId(args: Array[String]): Int = {
//    if (args == null || args.length <= 0) {
//      -1;
//    } else {
//      try {
//        args.apply(0).toInt
//      } catch {
//        case e: Exception =>
//          -1
//      }
//    }
//  }
//
//  /**
//    * 获取sparkContext
//    *
//    * @return
//    */
//
//  private def getSparkContext(): SparkContext = {
//    val sparkConf = new SparkConf().setAppName(sparkAppName).setMaster(sparkMaster)
//    sparkConf.set("spark.broadcast.factory", "org.apache.spark.broadcast.HttpBroadcastFactory")
//    sparkConf.set("spark.network.timeout", "300")
//    sparkConf.set("spark.streaming.unpersist", "true")
//    sparkConf.set("spark.scheduler.listenerbus.eventqueue.size", "100000")
//    sparkConf.set("spark.storage.memoryFraction", "0.5")
//    sparkConf.set("spark.shuffle.consolidateFiles", "true")
//    sparkConf.set("spark.shuffle.file.buffer", "64")
//    sparkConf.set("spark.shuffle.memoryFraction", "0.3")
//    sparkConf.set("spark.reducer.maxSizeInFlight", "24")
//    sparkConf.set("spark.shuffle.io.maxRetries", "60")
//    sparkConf.set("spark.shuffle.io.retryWait", "60")
//    sparkConf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    new SparkContext(sparkConf)
//  }
//
//  /**
//    * 获取sparkTask的任务参数集合
//    *
//    * @param taskId
//    * @return
//    */
//  private def getTaskParam(taskId: Int, sc: SparkContext): List[Tuple4[String, String, String, util.List[Filter]]] = {
//    var list: List[Tuple4[String, String, String, util.List[Filter]]] = List()
//    val sparkTask = sparkTaskService.getSparkTaskByTaskId(taskId)
//    val params = sparkTask.getQueryParam
//    val gson = new Gson
//    val sparkQueryParams = gson.fromJson(params, classOf[SparkQueryParams])
//    try {
//      //1.**
//      val systemId = sparkQueryParams.getSystemId
//      //2.开始时间
//      val startTime = sparkQueryParams.getStartTime
//      //3.结束时间
//      val endTime = sparkQueryParams.getEndTime
//      //4.**
//      val stationId = sparkQueryParams.getStationId
//      val paramList = sparkQueryParams.getParams
//      for (i <- 0 until paramList.size()) {
//        val param = paramList.get(i)
//        //5.**
//        val msgId = param.getMsgId
//        //6.**
//        val sinkId = param.getSinkId
//        //7.**
//        val sourceId = param.getSourceId
//        //8.表名
//        val tableName = msgService.getTieYuanMsgTableNameById(msgId);
//        for (num <- 0 until serviceNum) {
//          val rowkeyAndFilters = getRowkeyAndFilters(num, systemId, startTime, endTime, stationId, msgId, sinkId, sourceId, tableName)
//          list = rowkeyAndFilters :: list
//        }
//      }
//      list
//    } catch {
//      case e: Exception =>
//        LogUtil.writeLog(systemId, LogLevel.ERROR, LogType.NORMAL_LOG, systemName + " Error Info:任务参数异常。" + e)
//        null
//    }
//  }
//
//  /**
//    * hbase数据查询
//    */
//  private def queryHbaseData(taskParamList: List[(String, String, String, util.List[Filter])], sc: SparkContext): RDD[(ImmutableBytesWritable, Result)] = {
//    var rdd: RDD[(ImmutableBytesWritable, Result)] = null
//    val latch:CountDownLatch=new CountDownLatch(taskParamList.length)
//    val list: util.List[Future[RDD[Tuple2[ImmutableBytesWritable, Result]]]]=new util.ArrayList[Future[RDD[Tuple2[ImmutableBytesWritable, Result]]]]()
//    for (taskParam <- taskParamList) {
//      list.add(excutor.submit(new HbaseQueryThread(taskParam,sc,latch)))
//    }
//    import scala.collection.JavaConversions._
//    for(li <- list){
//      if(rdd==null){
//        rdd=li.get()
//      }else{
//        rdd=rdd.++(li.get())
//      }
//    }
//    latch.await()
//    rdd
//  }
//
//
//  /**
//    * 获取
//    *
//    * @param num
//    * @param systemId
//    * @param startTime
//    * @param endTime
//    * @param stationId
//    * @param msgId
//    * @param sinkId
//    * @param sourceId
//    * @return
//    */
//  private def getRowkeyAndFilters(num: Int, systemId: Int, startTime: String,
//                                  endTime: String, stationId: Int, msgId: Int,
//                                  sinkId: Int, sourceId: Int,
//                                  tableName: String): Tuple4[String, String, String, util.List[Filter]]
//
//  = {
//    //线程非安全,因此每次调用时创建新的对象
//    val simpleDateFormat1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")
//    val simpleDateFormat2 = new SimpleDateFormat("yyyyMMddHHmmssSSS")
//    val decimalFormat = new DecimalFormat("00")
//    val queryDef = new QueryDef
//    //1.设置数据库
//    queryDef.setDataBaseType(DataBaseType.HBASE)
//    //2.设置表名
//    queryDef.setTableName(tableName)
//    //3.设置请求参数集合
//    //3.1设置**Id参数
//    val systemIdParam = new QueryParam
//    systemIdParam.setField(new Field(new FieldInfo(RowkeyConstant.SYSTEM_ID), new FieldValue(systemId)))
//    systemIdParam.setCompareOp(CompareOp.EQUAL)
//    //3.2设置**
//    val msgIdParam = new QueryParam
//    msgIdParam.setField(new Field(new FieldInfo(RowkeyConstant.MSG_ID), new FieldValue(msgId)))
//    msgIdParam.setCompareOp(CompareOp.EQUAL)
//    //3.3设置开始时间参数
//    val startTimeParam = new QueryParam
//    val startTimeFormat = simpleDateFormat2.format(simpleDateFormat1.parse(startTime))
//    startTimeParam.setField(new Field(new FieldInfo(RowkeyConstant.TIME), new FieldValue(startTimeFormat)))
//    startTimeParam.setCompareOp(CompareOp.GREATER)
//    //3.4设置结束时间参数
//    val endTimeParam = new QueryParam
//    val endTimeFormat = simpleDateFormat2.format(simpleDateFormat1.parse(endTime))
//    endTimeParam.setField(new Field(new FieldInfo(RowkeyConstant.TIME), new FieldValue(endTimeFormat)))
//    endTimeParam.setCompareOp(CompareOp.LESS)
//    //3.5设置**
//    val sourceParam = new QueryParam
//    sourceParam.setField(new Field(new FieldInfo(RowkeyConstant.SINK_ID), new FieldValue(sinkId)))
//    sourceParam.setCompareOp(CompareOp.EQUAL)
//    //3.6设置**
//    val sinkParam = new QueryParam
//    sinkParam.setField(new Field(new FieldInfo(RowkeyConstant.SOURCE_ID), new FieldValue(sourceId)))
//    sinkParam.setCompareOp(CompareOp.EQUAL)
//    val queryParamList = util.Arrays.asList(systemIdParam, msgIdParam, startTimeParam, endTimeParam, sourceParam, sinkParam)
//    queryDef.setListQueryParam(queryParamList)
//    val startRowkey = decimalFormat.format(num) + queryDef.getStartRowKey(classOf[String])
//    val endRowkey = decimalFormat.format(num) + queryDef.getStopRowKey(classOf[String])
//    val filters = queryDef.getFilters(2, num, classOf[String])
//    new Tuple4(tableName, startRowkey, endRowkey, filters)
//  }
//
//  /**
//    * 进行hbase查询
//    *
//    * @param taskParam
//    * @param sc
//    */
//  def getHbaseQueryRDD(taskParam: (String, String, String, util.List[Filter]), sc: SparkContext): RDD[(ImmutableBytesWritable, Result)] = {
//    val hbaseConf = HBaseConfiguration.create()
//    hbaseConf.set(HbaseZookeeperConstant.HBASE_ZOOKEEPER_QUORUM, zookeeperHost)
//    hbaseConf.set(HbaseZookeeperConstant.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT, zookeeperPort)
//    hbaseConf.set(TableInputFormat.INPUT_TABLE, taskParam._1)
//    val scan = new Scan()
//    scan.setStartRow(Bytes.toBytes(taskParam._2))
//    scan.setStopRow(Bytes.toBytes(taskParam._3))
//    val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, taskParam._4)
//    scan.setFilter(filterList)
//    hbaseConf.set(TableInputFormat.SCAN, convertScanToString(scan))
//    val rs = sc.newAPIHadoopRDD(
//      hbaseConf,
//      classOf[TableInputFormat],
//      classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
//      classOf[org.apache.hadoop.hbase.client.Result])
//    //todo 解析
//    rs
//    //   rs.map(tuple2=>{
//    //     val result=tuple2._2
//    //     result.
//    //   })
//  }
//
//  private def convertScanToString(scan: Scan) = {
//    val proto = ProtobufUtil.toScan(scan)
//    Base64.encodeBytes(proto.toByteArray)
//  }
//}