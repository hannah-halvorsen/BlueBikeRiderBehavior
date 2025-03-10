package bluebikes

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.scaladsl._
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util.zip.ZipInputStream
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}
import scala.xml.XML

object BluebikesDownloader {
  implicit val system: ActorSystem = ActorSystem()
  implicit val executionContext: ExecutionContext = system.dispatcher

  def main(args: Array[String]): Unit = {
    val baseUrl = "https://s3.amazonaws.com/hubway-data/"
    val indexPage = baseUrl

    // Create output directory for unzipped files
    val outputDir = "./unpacked/"
    Files.createDirectories(Paths.get(outputDir))
    println(s"Created directory: $outputDir")


    println(s"Fetching file list from $indexPage...")

    // Fetch and parse the index page to get file names
    val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(uri = indexPage))

    responseFuture.onComplete {
      case Success(response) if response.status.isSuccess() =>
        response.entity.dataBytes.runFold("")(_ + _.utf8String).onComplete {
          case Success(xmlContent) =>
            val files = parseFileNamesFromXml(xmlContent)
            downloadFiles(baseUrl, files)
          case Failure(exception) =>
            println(s"Failed to parse XML: ${exception.getMessage}")
            system.terminate()
        }
      case Success(response) =>
        println(s"Failed to fetch index page. HTTP Status: ${response.status}")
        system.terminate()
      case Failure(exception) =>
        println(s"Failed to send request: ${exception.getMessage}")
        system.terminate()
    }
  }

  def parseFileNamesFromXml(xmlContent: String): Seq[String] = {
    val xml = XML.loadString(xmlContent)
    val fileNames = (xml \\ "Contents" \\ "Key").map(_.text).filter(_.endsWith(".zip")).toSeq

    // Debug: Print filtered .zip file names
    println("Filtered file names:")
    fileNames.foreach(println)

    fileNames
  }

  def downloadFiles(baseUrl: String, files: Seq[String]): Unit = {
    val downloadFutures = files.map { file =>
      val url = baseUrl + file
      val outputFile = Paths.get(file)

      // Skip downloading if the file already exists
      if (Files.exists(outputFile)) {
        println(s"File already exists, skipping download: $outputFile")
        // Unzip the file directly if it exists
        unzipFile(file, "./unpacked/")
        Future.successful(())
      } else {
        println(s"Downloading $url...")

        val responseFuture: Future[HttpResponse] = Http().singleRequest(HttpRequest(uri = url))

        responseFuture.flatMap {
          case response if response.status.isSuccess() =>
            val sink = FileIO.toPath(outputFile)
            response.entity.dataBytes.runWith(sink).map { _ =>
              println(s"Downloaded $file successfully.")
              // Unzip the file after downloading
              unzipFile(file, "./unpacked/")
            }
          case response =>
            Future.failed(new Exception(s"Failed to download file. HTTP Status: ${response.status}"))
        } recover {
          case exception =>
            println(s"Failed to download $file: ${exception.getMessage}")
        }
      }
    }

    // Wait for all downloads to complete
    Future.sequence(downloadFutures).onComplete { _ =>
      println("All downloads completed.")
      system.terminate()
    }
  }

  def unzipFile(zipFilePath: String, outputDir: String): Unit = {
    val zipInputStream = new ZipInputStream(Files.newInputStream(Paths.get(zipFilePath)))
    Stream.continually(zipInputStream.getNextEntry).takeWhile(_ != null).foreach { entry =>
      val filePath = Paths.get(outputDir, entry.getName)
      if (!entry.isDirectory) {
        Files.createDirectories(filePath.getParent)
        Files.copy(zipInputStream, filePath, StandardCopyOption.REPLACE_EXISTING)
        println(s"Extracted: ${filePath.toString}")
      }
    }
    zipInputStream.close()
  }
}
