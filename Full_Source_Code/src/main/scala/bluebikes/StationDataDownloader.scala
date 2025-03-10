package bluebikes

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.scaladsl._
import java.nio.file.{Files, Paths, StandardCopyOption}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object StationDataDownloader {
  implicit val system: ActorSystem = ActorSystem()
  implicit val executionContext: ExecutionContext = system.dispatcher

  def main(args: Array[String]): Unit = {
    val stationDataUrl = "https://gbfs.bluebikes.com/gbfs/en/station_information.json"
    val outputDir = "./station_data/"
    val outputFile = outputDir + "station_information.json"

    // Create output directory if it doesn't exist
    Files.createDirectories(Paths.get(outputDir))
    println(s"Created directory: $outputDir")

    // Download the station data
    println(s"Fetching station data from $stationDataUrl...")
    downloadStationData(stationDataUrl, outputFile).onComplete {
      case Success(_) =>
        println(s"Station data saved to $outputFile")
        system.terminate()
      case Failure(exception) =>
        println(s"Failed to download station data: ${exception.getMessage}")
        system.terminate()
    }
  }

  def downloadStationData(url: String, outputPath: String): Future[Unit] = {
    if (Files.exists(Paths.get(outputPath))) {
      println(s"File already exists: $outputPath, skipping download.")
      Future.successful(())
    } else {
      Http().singleRequest(HttpRequest(uri = url)).flatMap {
        case response if response.status.isSuccess() =>
          val sink = FileIO.toPath(Paths.get(outputPath))
          response.entity.dataBytes.runWith(sink).map(_ => ())
        case response =>
          Future.failed(new Exception(s"Failed to fetch station data. HTTP Status: ${response.status}"))
      }
    }
  }
}

