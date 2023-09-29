package Preprocessor

import scala.io.Source
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets

object TabReplacer {

  def replaceTabsWithSpaces(filePath: String): Unit = {
    // Read the file
    val content = Source.fromFile(filePath).mkString

    // Replace tabs with two spaces
    val updatedContent = content.replaceAll("\t", "  ")

    // Write the updated content back to the file
    Files.write(Paths.get(filePath), updatedContent.getBytes(StandardCharsets.UTF_8))
  }
}
