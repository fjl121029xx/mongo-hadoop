package com.li.zac

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.spark.config.ReadConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.collection.mutable.{ArrayBuffer, Map}

case class AnswerCard3(
                        userid: Long,
                        correct: Array[Int],
                        question: Array[Int],
                        point: Array[Int],
                        answertime: Array[Int],
                        createtime: String,
                        subject: Int,
                        partitiontime: String
                      )

object Download {

  def main(args: Array[String]): Unit = {


    //    var inputUrl = "mongodb://192.168.100.20:37017/huatu_ztk"
    //    var hive_outPut_table = "zac2"
    var inputUrl = "mongodb://huatu_ztk:wEXqgk2Q6LW8UzSjvZrs@192.168.100.153:27017,192.168.100.153:27017,192.168.100.155:27017/huatu_ztk"
    var hive_outPut_table = "tmp_zac"
    //
    if (args.length == 2) {

      inputUrl = args(0)
      hive_outPut_table = args(1)
    }

    //    //    inputUrl = "mongodb://192.168.100.20:37017/huatu_ztk"
    //    //    hive_outPut_table = "zac2"
    //
    val warehouseLocation = new File("spark-warehouse").getAbsolutePath
    System.setProperty("HADOOP_USER_NAME", "root")

    val conf = new SparkConf()
      .setAppName("mongo-ztk_answer_card")
      //      .setMaster("local")
      .set("hive.exec.dynamic.partition", "true")
      .set("hive.exec.dynamic.partition.mode", "nonstrict")
      .set("hive.exec.max.dynamic.partitions", "1800")
      .set("mapreduce.map.memory.mb", "10240")
      .set("mapreduce.reduce.memory.mb", "10240")
      .set("spark.mongodb.input.readPreference.name", "secondaryPreferred")
      .set("spark.mongodb.input.partitioner", "MongoPaginateBySizePartitioner")
      .set("spark.mongodb.input.partitionerOptions.partitionKey", "_id")
      .set("spark.mongodb.input.partitionerOptions.partitionSizeMB", "32")
      .set("spark.mongodb.input.samplesPerPartition", "5000000")
      .set("spark.debug.maxToStringFields", "100")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.debug.maxToStringFields", "100")
      .registerKryoClasses(Array(classOf[scala.collection.mutable.WrappedArray.ofRef[_]]))

    import com.mongodb.spark.sql._
    val sparkSession = SparkSession.builder().config("spark.sql.warehouse.dir", warehouseLocation).config(conf).enableHiveSupport().getOrCreate()


    val sc = sparkSession.sparkContext


    val ztk_question = sparkSession.loadFromMongoDB(
      ReadConfig(
        Map(
          "uri" -> inputUrl.concat(".ztk_question_new"),
          "readPreference.name" -> "secondaryPreferred",
          "partitioner" -> "MongoPaginateBySizePartitioner",
          "partitionerOptions.partitionSizeMB" -> "32",
          "maxBatchSize" -> "1000000",
          "keep_alive_ms" -> "500")
      )).toDF() // Uses the ReadConfig
    ztk_question.createOrReplaceTempView("ztk_question")
    //    val q2p = sc.broadcast(map.collectAsMap())
    /**
      * mongo 214024
      * spark 205846
      * the mapping of the knowledge to points
      */
    // spark context
    val q2p = sc.broadcast(sparkSession.sql("select _id,points from ztk_question").rdd.filter { r =>
      !r.isNullAt(0) && !r.isNullAt(1) && r.getSeq(1).nonEmpty
    }.map {
      r =>
        val _id: Int = r.get(0).asInstanceOf[Number].intValue()
        val pid = r.getSeq(1).head.toString.toDouble.intValue()
        (_id, pid)
    }.collectAsMap())

    println(q2p.value.size)
    if (q2p.value.isEmpty) {
      sparkSession.stop()
    }


    val ztk_answer_card = sparkSession.loadFromMongoDB(
      ReadConfig(
        Map(
          "uri" -> inputUrl.concat(".ztk_answer_card"),
          "readPreference.name" -> "secondaryPreferred",
          "partitioner" -> "MongoPaginateBySizePartitioner",
          "partitionerOptions.partitionSizeMB" -> "32"
        )
      )).toDF() // Uses the ReadConfig


    ztk_answer_card.printSchema()

    ztk_answer_card.createOrReplaceTempView("zac")
    val card = sparkSession.sql("select userId,corrects,paper.questions,times,createTime,subject,status from zac")

    import sparkSession.implicits._

    card.show()

    val zac = card.mapPartitions {


      ite: Iterator[Row] =>

        val q2pMap = q2p.value
        val format = new SimpleDateFormat("yyyyMMdd")
        val format2 = new SimpleDateFormat("yyyyMM")
        val arr = new ArrayBuffer[AnswerCard3]()

        val start = System.currentTimeMillis() - 2 * 24 * 60 * 60 * 1000L
        val end = System.currentTimeMillis()

        while (ite.hasNext) {

          val r = ite.next()


          try {

            val userId = r.getAs[Long](0).longValue()
            val corrects = r.getAs[Seq[Int]](1).toArray
            val questions = r.getAs[Seq[Int]](2).toArray
            val times = r.getAs[Seq[Int]](3).toArray
            val createTime = r.getAs[Long](4).longValue()
            val subject = r.getAs[Int](5).intValue()
            val status = r.getAs[Int](6).intValue()

            val points = new ArrayBuffer[Int]()
            questions.foreach { qid =>

              val pid: Int = q2pMap.getOrElse(qid, 0)
              points += pid
            }
            //
            if (createTime >= start && end >= createTime && status == 3) {

              arr += AnswerCard3(userId, corrects, questions, points.toArray, times, format.format(new Date(createTime)), subject,format2.format(new Date(createTime)))
            }
          } catch {
            case ex: NumberFormatException => {
              println(r)
            }
            case ex2: NullPointerException => {
              println(r)
            }
          }
        }
        arr.iterator
    }.toDF()

    zac.cache()
    zac.repartition(1).write.mode(SaveMode.Overwrite).partitionBy("partitiontime")
      .saveAsTable(hive_outPut_table)
  }

}
