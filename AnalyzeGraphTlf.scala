package tlf

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx._

/**
  * @author ${user.name}
  */
object AnalyzeGraphTlf {

  def main(args : Array[String]) {
    val conf = new SparkConf()
      .setAppName("Testing graphx")
      .setMaster("local")
      .setSparkHome("/usr/lib/spark")


    val sc = new SparkContext(conf)


    //“metadata-processed-tlf”, que contiene la lista de aristas o arcos.
    // Load the edges as a graph
    //The file should only contain two integers in each line, a source id and a target id.
    val graph = GraphLoader.edgeListFile(sc, "metadata-processed-tlf")

    // Run PageRank


    //0.0001 is our threshold for convergence.
    // After executing pagerank, we join the results against our dictionary file,
    //sort the results by the rank, and output the first five results.
    val ranks = graph.pageRank(0.0001).vertices


    val entities = sc.textFile("metadata-lookup-tlf").map
    {
      line =>val fields = line.split("\\s+")
       (fields(0).toLong, fields(1))

    }


    // join the ids with the phone numbers
    val ranksPorVertice = entities.join(ranks).map {
      case (id, (vertex, rank)) => (rank, vertex)
    }

    // print out the top 10 phone pagerank entities of our dataset
    println(ranksPorVertice.sortByKey(false).take(10).mkString("\n"))



  }

}
