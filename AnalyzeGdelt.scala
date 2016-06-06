package gdelt

import org.apache.spark.graphx.{Graph, Edge, EdgeTriplet, GraphLoader}
import org.apache.spark.{SparkContext, SparkConf}


import scala.xml.Null

/**
  * Created by cloudera on 2/23/16.
  */
object AnalyzeGdelt {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Testing graphx").setMaster("local[4]").setSparkHome("/usr/lib/spark")

    val sc = new SparkContext(conf)

        //graphx solo soporta
    //GraphX currently only supports integer vertex ids.
    //conversion del tipo de dato string a entero

    //“processed-gdelt”, que contiene la lista de aristas o arcos.
    // Load the edges as a graph
    //The file should only contain two integers in each line, a source id and a target id.

    val graph = GraphLoader.edgeListFile(sc,"hdfs://localhost:8020/user/cloudera/processed-gdelt")

    // Run PageRank
    //PageRank para detectar relaciones
    val ranks = graph.pageRank(0.0001).vertices

    // join the ids with the phone numbers
    //val entities = sc.textFile("metadata-lookup-gdelt").map
    val entities = sc.textFile("hdfs://localhost:8020/user/cloudera/lookup-gdelt").map
    {
      line =>val fields = line.split("\\s+")
        (fields(0).toLong, fields(1))
    }

    val ranksByVertex = entities.join(ranks).map {
      case (id, (vertex, rank)) => (rank, vertex)
    }
    // print out the top 5 entities
    println(ranksByVertex.sortByKey(false).take(10).mkString("\n"))



  }



}
