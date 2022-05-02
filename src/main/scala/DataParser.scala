import akka.actor.ActorSystem
import akka.stream.alpakka.csv.scaladsl.{CsvParsing, CsvToMap}
import akka.stream.scaladsl.FileIO

import java.io.File
import java.nio.file.{Path, Paths}
import scala.collection.immutable.ListMap
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

class DataParser (implicit val system: ActorSystem) {

  def calcStats(data: Map[String, String]) : Unit = {
    data.getOrElse("humidity","NaN") match {
      case "NaN" => DataParser.failureTotalMap += (data.getOrElse("sensor-id","NaN") -> (DataParser.failureTotalMap.getOrElse("sensor-id",0) + 1))
      case _ => {
        DataParser.minMap += (data.getOrElse("sensor-id","NaN") -> scala.math.min(DataParser.minMap.getOrElse(data.getOrElse("sensor-id","NaN"),99999),data.getOrElse("humidity","NaN").toInt))
        DataParser.maxMap += (data.getOrElse("sensor-id","NaN") -> scala.math.max(DataParser.maxMap.getOrElse(data.getOrElse("sensor-id","NaN"),0),data.getOrElse("humidity","NaN").toInt))
        DataParser.sumMap += (data.getOrElse("sensor-id","NaN") -> (DataParser.sumMap.getOrElse(data.getOrElse("sensor-id","NaN"),0) + data.getOrElse("humidity","NaN").toInt))
        DataParser.successTotalMap += (data.getOrElse("sensor-id","NaN") -> (DataParser.successTotalMap.getOrElse(data.getOrElse("sensor-id","NaN"),0) + 1))

      }
    }
  }

  def parseLeaderFile(dataFile: File): Future[Boolean] = {
    val csvFile: Path = Paths.get(dataFile.getPath)

    val source = FileIO.fromPath(csvFile)

    source
      .via(CsvParsing.lineScanner(','))
      .via(CsvToMap.toMapAsStrings())
      .runForeach(x => calcStats(x))
      .map{ _ =>
        DataParser.successTotalMap.foreach{ kv =>
          DataParser.avgMap += ( kv._1 -> (DataParser.sumMap.getOrElse(kv._1,0)/DataParser.successTotalMap.getOrElse(kv._1,1)))
        }
        DataParser.avgMap = ListMap(DataParser.avgMap.toSeq.sortWith(_._2 > _._2):_*) //Sort avgMap by Average Values in descending order


        val failedMeasurements: Int = (DataParser.failureTotalMap.foldLeft(0)(_+_._2)).toInt
        val processedMeasurements: Int = (DataParser.successTotalMap.foldLeft(0)(_+_._2)).toInt + failedMeasurements
        println("Num of processed measurements: "+ processedMeasurements)
        println("Num of failed measurements: "+ failedMeasurements)
        println("")

        println("Sensors with highest avg humidity:")
        println("")
        println("sensor-id,min,avg,max")
        DataParser.avgMap.foreach{kv =>
          printf("%s,%s,%s,%s \n",kv._1,DataParser.minMap.getOrElse(kv._1,0),kv._2.toInt,DataParser.maxMap.getOrElse(kv._1,0))

        }
        DataParser.failureTotalMap.foreach{kv =>
          if(!DataParser.successTotalMap.contains(kv._1)){printf("%s,%s,%s,%s \n",kv._1,"NaN","NaN","NaN")}
        }
        true
      }
      .recover {
        case _: Exception => println("Some Exception occurred while processing")
          false
      }


  }

}

object DataParser{
  private var minMap: Map[String, Int] = Map()
  private var maxMap: Map[String, Int] = Map()
  private var sumMap: Map[String, Int] = Map()
  private var successTotalMap: Map[String, Int] = Map()
  private var failureTotalMap: Map[String, Int] = Map()
  private var avgMap: Map[String, Float] = Map()
}
