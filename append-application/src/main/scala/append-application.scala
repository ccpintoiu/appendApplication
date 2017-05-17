import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.{StructType, StructField, StringType};
import org.apache.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.size
import com.univocity.parsers.csv._
import org.apache.hadoop.fs._
import org.apache.hadoop.conf._
import sys.process._

object appendObject{

  def main(args: Array[String]): Unit = {
   if (args.length != 7) {
      System.err.println(s"""
        |Usage: append <microbatch_interval> <application_name> <spark_master> <kafka_brokers> <kafka_topics>
        |  <microbatch_interval> is the amount of time in seconds between two succesive runs of the job
        |  <application_name> is the name of the application as it will appear in YARN. It will be prefixed with "microbatch"
        |  <spark_master> is the connection string to the application master
        |  <kafka_brokers> is the connection string to the kafka kafka_brokers
        |  <kafka_topic> is the name of the topic to read from
        |  <kafka_offset> is the offset to start from: latest OR earliest
        |  <kafka_offset> this is the destination url to save the table. Can be file, hdfs, bigstep datalake
                """.stripMargin)
      System.exit(1)
    }
        //read arguments
        val Array(micro_time,app_name, spark_master, kafka_brokers, kafka_topic, kafka_offset,url) = args

        //intialize Spark Session
        val spark = SparkSession.builder().master(spark_master).appName("streaming"+app_name).enableHiveSupport().getOrCreate()

        import spark.implicits._
        val lines = dataStream.selectExpr("CAST(value AS STRING)").as[(String)]
        //Write to Table
       val write_to_table = lines.writeStream.outputMode("append").format("parquet").queryName(kafka_topic).option("path",url).option("checkpointLocation","/checkpoint").start()
        //val write_to_table = lines.writeStream.outputMode("append").format("memory").queryName(kafka_topic).start()

        //Write Memory table to Physical SparkSQL tables periodically
        //val selectall_query = "SELECT * FROM "
        //val timer = new Timer("Rewrite Table", true)
        //timer.schedule(new TimerTask{
          //      override def run() {
            //            spark.sql(selectall_query+kafka_topic).write.format("parquet").mode("overwrite").saveAsTable("managed_table_"+kafka_topic)
              //  }
        //}, batch_interval, batch_interval)
        write_to_table.awaitTermination()
        }

}
