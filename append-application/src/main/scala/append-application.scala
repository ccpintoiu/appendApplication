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
   if (args.length != 5) {
      System.err.println(s"""
        datalake
                """.stripMargin)
      System.exit(1)
    }
        //read arguments
        val Array(app_name, spark_master, url, dest_url , table_to_update) = args

        //intialize Spark Session
        val spark = SparkSession.builder().master(spark_master).appName(app_name).enableHiveSupport().getOrCreate()

        import spark.implicits._
    
        val data = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", "|").option("parserLib", "univocity").csv(url)
       
        //Write to Table
       val write_to_table = data.write.mode("append").format("parquet").option("path",dest_url).saveAsTable(table_to_update)
       
       
}
