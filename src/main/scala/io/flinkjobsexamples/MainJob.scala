package io.flinkjobsexamples

import java.util.concurrent.TimeUnit

import io.flinkjobsexamples.events.{CardAccountEnrichedElement, CardEvent}
import io.flinkjobsexamples.sources.{AsyncDatabaseRequest, CardEventSource}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

//https://github.com/apache/flink/blob/master/flink-streaming-scala/src/test/scala/org/apache/flink/streaming/api/scala/AsyncDataStreamITCase.scala

object MainJob {

  private var testResult: mutable.ArrayBuffer[CardAccountEnrichedElement] = _
  private val PAUSE = 100

  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(this.getClass)

    val params = ParameterTool.fromArgs(args)
    val parallelism = params.getInt("parallelism", 1)

    logger.info("Params: " + params.toMap.toString)

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    implicit val typeInfoCard = TypeInformation.of(classOf[(CardEvent)])
    implicit val typeInfoAccount = TypeInformation.of(classOf[(CardAccountEnrichedElement)])

    val inputEventStream: DataStream[CardEvent] = env.addSource(new CardEventSource(PAUSE))
    val withTimestampsAndWatermarks: DataStream[CardEvent] = inputEventStream.assignAscendingTimestamps(_.getCreationTime)

    val timeout = 100L
    val asyncMapped: DataStream[CardAccountEnrichedElement] =
      AsyncDataStream.orderedWait(withTimestampsAndWatermarks, new AsyncDatabaseRequest(), timeout, TimeUnit.MILLISECONDS)

    testResult = mutable.ArrayBuffer[CardAccountEnrichedElement]()
    asyncMapped.addSink((cardAccountEnrichedElement: CardAccountEnrichedElement) => {
      logger.info("cardAccountEnrichedElement: " + cardAccountEnrichedElement.toString)
      testResult.append(cardAccountEnrichedElement)
    })

    env.execute("testAsyncDataStream")
  }

}
