package Preprocessor

import NetGraphAlgebraDefs.{NetGraph, NodeObject}
import org.slf4j.{Logger, LoggerFactory}

import java.io.{BufferedWriter, FileWriter}
import java.util
import java.util.Set
import ShardCreatorForNodes.maptonode
object NodeExporter {

  def exportNodesData(ngsPath:String, ngsPathPerturbed: String, outPath: String): Unit = {
    val logger: Logger = LoggerFactory.getLogger("ShardCreatorForNodes")

    logger.info("Started  Node Processing")

    val graph: Option[NetGraph] = NetGraph.load(ngsPath)

    val perturbedGraph: Option[NetGraph] = NetGraph.load(ngsPathPerturbed)

    val nodes = graph.get.sm.nodes().toArray().toList

    val perturbedNodes = perturbedGraph.get.sm.nodes().toArray().toList

    val customNodes = nodes.map(
      x => maptonode(x.asInstanceOf[NetGraphAlgebraDefs.NodeObject], graph, "original")
    )

    val customPerturbedNodes = perturbedNodes.map(
      x => maptonode(x.asInstanceOf[NetGraphAlgebraDefs.NodeObject], perturbedGraph, "perturbed")
    )

    val bufferedWriterObject = new BufferedWriter(new FileWriter(outPath+"_NodesOut" + ".txt"))
    val bufferedWriterObject2 = new BufferedWriter(new FileWriter(outPath+"_NodesPerturbedOut" + ".txt"))
     // save in text file

     customNodes.foreach(y => {
         bufferedWriterObject.write(y.toString)
         bufferedWriterObject.write("\n")
       }
     )

    customPerturbedNodes.foreach(y => {
         bufferedWriterObject2.write(y.toString)
         bufferedWriterObject2.write("\n")
       }
    )

    bufferedWriterObject.close()
    bufferedWriterObject2.close()
  }


  def main(args: Array[String]): Unit = {

    // Assuming that the first three command line arguments are the required paths
    if (args.length < 3) {
      println("Usage: NodeExporter <path_to_original_graph> <path_to_perturbed_graph> <path_to_output>")
//      System.exit(1)
    }

//    val ngsPath = args(0)
//    val ngsPathPerturbed = args(1)
//    val outPath = args(2)

    val ngsPath = "./../Homework1/NetGameSim300NPoint3.ngs"
    val ngsPathPerturbed = "./../Homework1/NetGameSim300NPoint3.ngs.perturbed"
    val outPath = "./out"

    exportNodesData(ngsPath, ngsPathPerturbed, outPath)
  }
}
