package Preprocessor

import NetGraphAlgebraDefs.{NetGraph, NodeObject}
import org.slf4j.{Logger, LoggerFactory}

import java.io.{BufferedWriter, FileWriter}
import java.util
import java.util.Set

object ShardCreatorForNodes {

  class node_data(val id: Int, val incoming_nodes_len: Int, val outgoing_node_len: Int, val children_props_hash: List[Int], val properties: List[Int], val valuableData: Boolean ,val graphType: String = "original") {

    def get_id(): Int = id

    def get_number_of_incoming_nodes(): Int = incoming_nodes_len

    def get_number_of_outgoing_nodes(): Int = outgoing_node_len

    def get_children_props_hash(): List[Int] = children_props_hash

    def get_properties(): List[Int] = properties

    def get_valuableData(): Boolean = valuableData

    override def toString: String = s"($id, $incoming_nodes_len, $outgoing_node_len, $children_props_hash, $properties, $valuableData)"

  }

  def maptonode(node:NetGraphAlgebraDefs.NodeObject, graph: Option[NetGraph], graphType: String):node_data = {
    val id = node.id
    // create a list with node ids from predecessors
    val incoming_node_len =  graph.get.sm.predecessors(node).size()
    val outgoing_node_len = graph.get.sm.successors(node).size()
    val children_props_hash = node.childrenObjects.map(x => x.hashCode()).toList
    val properties = node.properties
    val valuableData = node.valuableData


    val new_node = new node_data(id,incoming_node_len,outgoing_node_len,children_props_hash,properties, valuableData,"original")

    //  println(new_node)

    new_node
  }

  def createShardedFileFromGraph(ngsPath: String, ngsPathPerturbed: String, outPath: String):Unit = {
    val logger: Logger = LoggerFactory.getLogger("ShardCreatorForNodes")

    logger.info("Started Sharding Graph for Node Processing")

    val graph: Option[NetGraph] = NetGraph.load(ngsPath)

    val perturbedGraph: Option[NetGraph] = NetGraph.load(ngsPathPerturbed)

    val nodes = graph.get.sm.nodes()

    val graphLen = nodes.size()


    // create 5% shard files from the graph
    val indexesArray = for (i <- 0 to 20) yield ((i * graphLen * (5.0 / 100.0)))

    val shardArray = indexesArray.sliding(2).map(x => nodes.toArray().slice(x(0).toInt, x(1).toInt).toList).toList

    shardArray.foreach(x => println(x.length))

    assert(shardArray.map(x => x.length).sum == graphLen)

    val perturbedNodes = perturbedGraph.get.sm.nodes()

    val perturbedGraphLen = perturbedNodes.size()

    //  println(perturbedGraphLen)

    val perturbedIndexesArray = for (i <- 0 to 20) yield ((i * perturbedGraphLen * (5.0 / 100.0)))

    val perturbedShardArray = perturbedIndexesArray.sliding(2).map(x => perturbedNodes.toArray().slice(x(0).toInt, x(1).toInt).toList).toList

    perturbedShardArray.foreach(x => println(x.length))

    assert(perturbedShardArray.map(x => x.length).sum == perturbedGraphLen)

    // for each shard in shard array make node data and store its string representation in a file
    val nodesDataArray = shardArray.map(x => x.map(x => maptonode(x.asInstanceOf[NetGraphAlgebraDefs.NodeObject], graph, "original")).toList)

    val perturbedNodesDataArray = perturbedShardArray.map(x => x.map(x => maptonode(x.asInstanceOf[NetGraphAlgebraDefs.NodeObject], perturbedGraph, "perturbed")).toList)

    // cartesian product of nodesDataArray and perturbedNodesDataArray
    val cartesianProduct = nodesDataArray.flatMap(x => perturbedNodesDataArray.map(y => (x, y)))

    // line indexed file output
    val bufferedWriterObject = new BufferedWriter(new FileWriter(outPath+"_shardedNodes" + ".txt"))

    cartesianProduct.foreach(x => {
      x._1.foreach(y => {
        bufferedWriterObject.write(y.toString)
        bufferedWriterObject.write("\t")
      })
      bufferedWriterObject.write("|")
      x._2.foreach(y => {
        bufferedWriterObject.write(y.toString)
        bufferedWriterObject.write("\t")
      })
      bufferedWriterObject.write("\n")
    })

    bufferedWriterObject.close()
  }
}
