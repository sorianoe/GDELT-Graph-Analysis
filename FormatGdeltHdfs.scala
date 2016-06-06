package gdelt

import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by cloudera on 5/19/16.
  */
object FormatGdeltHdfs {



  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Clean data").setMaster("local").setSparkHome("/usr/lib/spark")

    val sc = new SparkContext(conf)
    val hadoopConf = new org.apache.hadoop.conf.Configuration()

    val hdfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI("hdfs://localhost:8020"), hadoopConf)


    //values with the 2 directories path to overwrite directories in hdfs
    val gdelt1 = "hdfs://localhost:8020/user/cloudera/lookup-gdelt"

    val gdelt2 = "hdfs://localhost:8020/user/cloudera/processed-gdelt"



    var vertices = sc.textFile("hdfs://localhost:8020/user/cloudera/dsnum_0.csv").flatMap { line => line.split("\\s+") }.distinct()

    try { hdfs.delete(new org.apache.hadoop.fs.Path(gdelt1), true) } catch { case _ : Throwable => { } }

    vertices.map { vertex => vertex.replace(",", "") + "\t" + vertex }.saveAsTextFile(gdelt1)


    val aristas=sc.textFile("hdfs://localhost:8020/user/cloudera/dsnum_0.csv").map { line => var fields = line.split(",")

      fields(0).replace(",", "") + "\t" + fields(1).replace(",", "")

    }
    try { hdfs.delete(new org.apache.hadoop.fs.Path(gdelt2), true) } catch { case _ : Throwable => { } }
      aristas.saveAsTextFile(gdelt2)


  }

}
