package Preprocessor

import NetGraphAlgebraDefs.{NetGraph, NodeObject}
import com.google.common.graph.EndpointPair
import org.slf4j.{Logger, LoggerFactory}

import java.io.{BufferedWriter, FileWriter}
import java.util
import java.util.Set

object ShardCreatorForEdges {

  class Edge(val src: NodeObject, val dst: NodeObject, val weight: Double) {
    val source_id = src.id
    val destination_id = dst.id
    val propertiesSource = src.properties
    val propertiesDestination = dst.properties
    val valuableDataSource = src.valuableData
    val valuableDataDestination = dst.valuableData
    val children_prop_hash_source = src.childrenObjects.map(x => x.hashCode()).toList
    val children_prop_hash_destination = dst.childrenObjects.map(x => x.hashCode()).toList


    override def toString: String = {
      s"($source_id, $destination_id, $weight, $propertiesSource, $propertiesDestination, $children_prop_hash_source, $children_prop_hash_destination, $valuableDataSource, $valuableDataDestination)"
    }
  }

  def createShardedEdgesFileFromGraph(ngsPath: String, ngsPathPerturbed: String, outPath: String): Unit = {
    val logger: Logger = LoggerFactory.getLogger("Main")

    logger.info("Hello World!")

    val graph: Option[NetGraph] = NetGraph.load(ngsPath)

    val perturbedGraph: Option[NetGraph] = NetGraph.load(ngsPathPerturbed)

    val edges = graph.get.sm.edges

    val perturbedEdges = perturbedGraph.get.sm.edges

    val edgesLen = edges.size

    val perturbedEdgesLen = perturbedEdges.size

    val indexesArray = for (i <- 0 to 20) yield ((i * edgesLen) / 20.0)

    val shardedArray = indexesArray.sliding(2).map(x => edges.toArray().slice(x(0).toInt, x(1).toInt).toList).toList

    val perturbedIndexesArray = for (i <- 0 to 20) yield ((i * perturbedEdgesLen) / 20.0)

    val perturbedShardedArray = perturbedIndexesArray.sliding(2).map(x => perturbedEdges.toArray().slice(x(0).toInt, x(1).toInt).toList).toList

    assert(shardedArray.map(x => x.size).sum == edgesLen)

    assert(perturbedShardedArray.map(x => x.size).sum == perturbedEdgesLen)

    println("edgesLen: " + edgesLen)

    val shardedEdges = shardedArray.map(x => x.map(y =>
      new Edge(y.asInstanceOf[EndpointPair[NodeObject]].nodeU(), y.asInstanceOf[EndpointPair[NodeObject]].nodeV(),
        graph.get.sm.edgeValue(y.asInstanceOf[EndpointPair[NodeObject]].nodeU(), y.asInstanceOf[EndpointPair[NodeObject]].nodeV()).get.cost)))

    val perturbedShardedEdges = perturbedShardedArray.map(x => x.map(y => new Edge(y.asInstanceOf[EndpointPair[NodeObject]].nodeU(), y.asInstanceOf[EndpointPair[NodeObject]].nodeV(),
      perturbedGraph.get.sm.edgeValue(y.asInstanceOf[EndpointPair[NodeObject]].nodeU(), y.asInstanceOf[EndpointPair[NodeObject]].nodeV()).get.cost)))

    // cartesian product of nodesDataArray and perturbedNodesDataArray
    val cartesianProduct = shardedEdges.flatMap(x => perturbedShardedEdges.map(y => (x, y)))

    // line indexed file output
    val bw3 = new BufferedWriter(new FileWriter(outPath + "_shardedEdges" + ".txt"))

    cartesianProduct.foreach(x => {
      x._1.foreach(y => {
        bw3.write(y.toString)
        bw3.write("\t")
      })
      bw3.write("|")
      x._2.foreach(y => {
        bw3.write(y.toString)
        bw3.write("\t")
      })
      bw3.write("\n")
    })

    bw3.close()
  }

}
