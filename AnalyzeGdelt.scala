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

      val graph = GraphLoader.edgeListFile(sc,"hdfs://localhost:8020/user/cloudera/processed-gdelt")

    // Run PageRank
    //PageRank para detectar relaciones
    val ranks = graph.pageRank(0.0001).vertices

    // join the ids with the gdelt numbers
    val entities = sc.textFile("hdfs://localhost:8020/user/cloudera/lookup-gdelt").map
    {
      line =>val fields = line.split("\\s+")
        (fields(0).toLong, fields(1))
    }

    val ranksPorVertice = entities.join(ranks).map {
      case (id, (vertex, rank)) => (rank, vertex)
    }
    // print out the top 10 entities
    println(ranksPorVertice.sortByKey(false).take(10).mkString("\n"))


    //Estadisticas GRAFO

    val nvertices=graph.numVertices ;
    println("Vertices Grafo:",nvertices);

    val naristas= graph.numEdges;
    println("Aristas Grafo:",naristas);

  }

}
