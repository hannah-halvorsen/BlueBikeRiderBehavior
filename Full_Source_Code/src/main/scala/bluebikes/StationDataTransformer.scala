package bluebikes

import java.nio.file.{Files, Paths}
import scala.io.Source
import spray.json._
import java.io.PrintWriter

// Case class to represent station data
case class Station(station_id: String, name: String, lat: Double, lon: Double, capacity: Int)

// JSON protocol for parsing
object StationJsonProtocol extends DefaultJsonProtocol {
  implicit val stationFormat: RootJsonFormat[Station] = jsonFormat5(Station)
}

object StationDataTransformer {
  import StationJsonProtocol._

  def main(args: Array[String]): Unit = {
    val inputFile = "./station_data/station_information.json"
    val outputFile = "./station_data/station_data.csv"

    // Read the JSON file
    val source = Source.fromFile(inputFile)
    val jsonContent = try source.mkString finally source.close()

    // Parse the JSON
    val stationsJson = jsonContent.parseJson.asJsObject
    val stations = stationsJson.fields("data").asJsObject.fields("stations").convertTo[Seq[Station]]

    // Write to CSV
    val writer = new PrintWriter(outputFile)
    writer.println("station_id,name,lat,lon,capacity") // CSV header
    stations.foreach { station =>
      writer.println(s"${station.station_id},${station.name},${station.lat},${station.lon},${station.capacity}")
    }
    writer.close()

    println(s"Station data transformed and saved to $outputFile")
  }
}
