package Preprocessor

import Preprocessor.ShardCreatorForEdges.createShardedEdgesFileFromGraph
import Preprocessor.ShardCreatorForNodes.createShardedFileFromGraph
import Preprocessor.TabReplacer.replaceTabsWithSpaces
import org.slf4j.{Logger, LoggerFactory}

object Main {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      logger.error("Four string parameters are required.")
      System.exit(1)
    }

    createShardedFileFromGraph(args(0), args(1), args(2))
    createShardedEdgesFileFromGraph(args(0), args(1), args(2))

    // Replace tabs with two spaces in the file specified by the third argument
    replaceTabsWithSpaces(args(3))
  }
}
