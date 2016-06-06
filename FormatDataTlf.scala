package tlf

import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ${user.name}
 */
object FormatDataTlf {

  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("Clean data").setMaster("local").setSparkHome("/usr/lib/spark")

    val sc = new SparkContext(conf)

  
     //    Read a text file from HDFS, a local file system (available on all nodes),
     //or any Hadoop-supported file system URI, and return it as an RDD of Strings.

    //GraphX takes an “edge list file” in which each line is an edge and an edge is a space separated pair of vertices.
    // Each vertex is represented by a 64-bit integer which should be unique to that vertex.


    //VERTICES
    var vertices = sc.textFile("dstlf.txt").flatMap { line => line.split("\\s+") }.distinct()
   // var vertices = sc.textFile("hdfs:///user/cloudera/dstlf.txt").flatMap { line => line.split("\\s+") }.distinct()

    vertices.map { vertex => vertex.replace("-", "") + "\t" + vertex }.saveAsTextFile("metadata-lookup-tlf")

    //we need a “lookup” or dictionary file to make use of the results of pagerank.
    //As such we also generate a “metadata-lookup-tlf” file,


    //This program outputs two directories, the edge list, which we named “metadata-processed”:

    //ARISTAS
    sc.textFile("dstlf.txt").map { line =>
      var fields = line.split("\\s+")
      if (fields.length == 2) {
        fields(0).replace("-", "") + "\t" + fields(1).replace("-", "")
      }
    }.saveAsTextFile("metadata-processed-tlf")  //contains the edge list file //las 2 columnas de numeros sin guiones

  }
  
}
