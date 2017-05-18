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
import org.apache.hadoop.conf.Configuration
import sys.process._
import java.io.File

object appendApplication{

  def main(args: Array[String]): Unit = {
   if (args.length != 7) {
      System.err.println(s"""
        Usage: JSON2Parquet <application_name> <spark_master> <source_directory> <file_to_update> <table_to_update> <backup_location> <json_file>
	         |  <application_name> is the name of the spark job
           |  <spark_master> is the spark master to connect to, "yarn" for example
           |  <source_directory> is the HDFS source location of the files that contain updates
           |  <file_to_update> is the HDFS location of the file to be updated (eg: hdfs://instance-29233.bigstep.io/user/test/dest.parquet)
	         |  <table_to_update> is the metastore table corresponding to the file_to_update parameter
           |  <backup_location> is the HDFS location of the directory where the files containing updates will be stored for future reference
           |  <json_file> is the HDFS location of the json file containing the updates to be pushed in zoomdata
                """.stripMargin)
      System.exit(1)
    }
    //read arguments
    val Array(application_name, spark_master, source_directory, file_to_update , table_to_update, backup_location, json_file) = args

    //intialize Spark Session
    val spark = SparkSession.builder().master(spark_master).appName(application_name).enableHiveSupport().getOrCreate()
    import spark.implicits._
    
    //infinite loop 
    while(true){
      //retrieve list of all files in the source_directory
      val path = new Path(source_directory)
      val fs = path.getFileSystem(new Configuration())
      val files = fs.listStatus(path).map(_.getPath).toList
    
      files.foreach{ file_path =>
        //for each file perform read, clean, update, backup and processing operations
        val file_name = file_path.getPath.getName
    
        //read the contents of the file in a dataframe
        val data = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", "|").option("parserLib", "univocity").csv(file_path)
      
       //preprocess data
        data.createOrReplaceTempView("dummy_table")
        val trimmed_data = spark.sql("select ItemID as id, ItemDescription as description, RegularSalesUnitPrice as price, LineItemSaleQuantity as quantity, TransactionBeginDateTime as date from dummy_table")
      
        //write to table and update file
        val write_to_table = trimmed_data.write.mode("append").format("parquet").option("path",file_to_update).saveAsTable(table_to_update)
      
        //write to json location for zoomdata update
        val write_to_json = trimmed_data.write.mode("append").format("org.apache.spark.sql.json").save(json_file)
       
        //push data in json to Zoomdata
      
        //remove temporary table from metastore
        spark.sql("drop table dummy_table")
      
        //remove json file
        fs.delete(new Path(json_file)) 
      
        //move file containing updates to backup location
        fs.rename(file_path, new Path(backup_location + file_name))
      }
      Thread sleep 30 
    }       
}
}
