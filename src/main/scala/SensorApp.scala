import akka.actor.ActorSystem
import java.io.File
import scala.concurrent.ExecutionContext.Implicits.global

object SensorApp {

  val usage =
    """
      |Usage: SensorApp <InputFilesDirectory>
      |""".stripMargin
  def main(args: Array[String]): Unit = {

    implicit val system: ActorSystem = ActorSystem()

    if(args.length == 0 || args.length > 1) {
      println(usage)
      system.terminate()
    }

    val dataFile = new File(args(0))

    new DataParser().parseLeaderFile(dataFile)
      .map { x =>
        if(x) println("") else println("File processing failed")
        system.terminate()
      }
      .recover{
        case _:Exception => println("Some Exception occurred while processing")
          system.terminate()
      }

  }
}
