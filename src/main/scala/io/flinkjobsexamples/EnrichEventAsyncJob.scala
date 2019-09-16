package io.flinkjobsexamples

import java.util.concurrent.TimeUnit

import io.flinkjobsexamples.functions.EnrichCardElementAsyncRequestFunction
import io.flinkjobsexamples.elements.{CardAccountElement, CardElement}
import io.flinkjobsexamples.sources.CardElementSource
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

object EnrichEventAsyncJob {

  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    val testResult: mutable.ArrayBuffer[CardAccountElement] = mutable.ArrayBuffer[CardAccountElement]()
    
    implicit val typeInfoCard: TypeInformation[CardElement] = TypeInformation.of(classOf[(CardElement)])
    implicit val typeInfoAccount: TypeInformation[CardAccountElement] = TypeInformation.of(classOf[(CardAccountElement)])

    val parallelism = 2
    val numEvents = 100
    val sourcePause = 50
    val ordered = false
    val asyncTimeout = 100L

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parallelism)

    val inputEventStream: DataStream[CardElement] = env.addSource(new CardElementSource(numEvents, sourcePause))

    val asyncMapped: DataStream[CardAccountElement] = if (ordered) {
      AsyncDataStream.orderedWait(inputEventStream, new EnrichCardElementAsyncRequestFunction(), asyncTimeout, TimeUnit.MILLISECONDS)
    }
    else {
      AsyncDataStream.unorderedWait(inputEventStream, new EnrichCardElementAsyncRequestFunction(), asyncTimeout, TimeUnit.MILLISECONDS)
    }

    asyncMapped.addSink((cardAccountEnrichedElement: CardAccountElement) => {
      logger.info("cardAccountEnrichedElement: " + cardAccountEnrichedElement.toString)
      testResult.append(cardAccountEnrichedElement)
    })

    env.execute("testAsyncDataStream")
  }

}
