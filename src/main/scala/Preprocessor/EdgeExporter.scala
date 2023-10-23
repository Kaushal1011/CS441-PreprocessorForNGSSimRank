package Preprocessor
import NetGraphAlgebraDefs.{NetGraph, NodeObject}
import org.slf4j.{Logger, LoggerFactory}
import com.google.common.graph.EndpointPair
import java.io.{BufferedWriter, FileWriter}
import java.util
import java.util.Set
import ShardCreatorForEdges.Edge
object EdgeExporter {

  def exportEdgesData(ngsPath: String, ngsPathPerturbed: String, outPath: String ): Unit = {
    val logger: Logger = LoggerFactory.getLogger("ShardCreatorForNodes")

    logger.info("Started Edge Processing")

    val graph = NetGraph.load(ngsPath)
    val perturbedGraph = NetGraph.load(ngsPathPerturbed)

    val edges = graph.get.sm.edges().toArray().toList

    val perturbedEdges = perturbedGraph.get.sm.edges().toArray().toList

    val customEdges = edges.map(
      y => new Edge(
        y.asInstanceOf[EndpointPair[NodeObject]].nodeU(), y.asInstanceOf[EndpointPair[NodeObject]].nodeV(), 1.0)
    )

    val customPerturbedEdges = perturbedEdges.map(
      y => new Edge(
        y.asInstanceOf[EndpointPair[NodeObject]].nodeU(), y.asInstanceOf[EndpointPair[NodeObject]].nodeV(),1.0)
    )

    val bufferedWriterObject = new BufferedWriter(new FileWriter(outPath + "_EdgesOut" + ".txt"))
    val bufferedWriterObject2 = new BufferedWriter(new FileWriter(outPath + "_EdgesPerturbedOut" + ".txt"))
    // save in text file

    customEdges.foreach(y => {
      bufferedWriterObject.write(y.toString)
      bufferedWriterObject.write("\n")
    }
    )

    customPerturbedEdges.foreach(y => {
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

    exportEdgesData(ngsPath, ngsPathPerturbed, outPath)
  }


}
