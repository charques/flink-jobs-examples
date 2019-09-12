package io.flinkjobsexamples.asyncdatastream

import java.util.concurrent.TimeUnit

import io.flinkjobsexamples.asyncdatastream.events.{CardAccountEnrichedElement, CardEvent}
import io.flinkjobsexamples.asyncdatastream.sources.{CardEventSource, EnrichCardEventAsyncRequestFunction}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

object EnrichEventAsyncJob {

  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    val testResult: mutable.ArrayBuffer[CardAccountEnrichedElement] = mutable.ArrayBuffer[CardAccountEnrichedElement]()
    implicit val typeInfoCard: TypeInformation[CardEvent] = TypeInformation.of(classOf[(CardEvent)])
    implicit val typeInfoAccount: TypeInformation[CardAccountEnrichedElement] = TypeInformation.of(classOf[(CardAccountEnrichedElement)])

    val parallelism = 2
    val numEvents = 100
    val sourcePause = 10
    val ordered = false
    val asyncTimeout = 100L

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)

    val inputEventStream: DataStream[CardEvent] = env.addSource(new CardEventSource(numEvents, sourcePause))
    val withTimestampsAndWatermarks: DataStream[CardEvent] = inputEventStream.assignAscendingTimestamps(_.getCreationTime)

    val asyncMapped: DataStream[CardAccountEnrichedElement] = if (ordered) {
      AsyncDataStream.orderedWait(withTimestampsAndWatermarks, new EnrichCardEventAsyncRequestFunction(), asyncTimeout, TimeUnit.MILLISECONDS)
    }
    else {
      AsyncDataStream.unorderedWait(withTimestampsAndWatermarks, new EnrichCardEventAsyncRequestFunction(), asyncTimeout, TimeUnit.MILLISECONDS)
    }

    asyncMapped.addSink((cardAccountEnrichedElement: CardAccountEnrichedElement) => {
      logger.info("cardAccountEnrichedElement: " + cardAccountEnrichedElement.toString)
      testResult.append(cardAccountEnrichedElement)
    })

    env.execute("testAsyncDataStream")
  }

}
