package Preprocessor

import Preprocessor.Main.getClass
import Preprocessor.ShardCreatorForEdges.createShardedEdgesFileFromGraph
import Preprocessor.ShardCreatorForNodes.createShardedFileFromGraph
import org.slf4j.{Logger, LoggerFactory}

object Main {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    if (args.length < 3) {
      logger.error("Three string parameters are required.")
      System.exit(1)
    }

    createShardedFileFromGraph(args(0), args(1), args(2))
    createShardedEdgesFileFromGraph(args(0), args(1), args(2))

  }

}