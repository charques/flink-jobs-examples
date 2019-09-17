package io.flinkjobsexamples

import java.util.concurrent.TimeUnit

import com.codahale.metrics.{ConsoleReporter, SharedMetricRegistries}
import io.flinkjobsexamples.functions.EnrichCardElementAsyncRequestFunction
import io.flinkjobsexamples.elements.{CardAccountElement, CardElement}
import io.flinkjobsexamples.sources.CardElementSource
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment
import org.apache.flink.streaming.api.scala.{AsyncDataStream, DataStream, StreamExecutionEnvironment}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

object EnrichEventAsyncJob {

  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    val testResult: mutable.ArrayBuffer[CardAccountElement] = mutable.ArrayBuffer[CardAccountElement]()

    implicit val typeInfoCard: TypeInformation[CardElement] = TypeInformation.of(classOf[(CardElement)])
    implicit val typeInfoAccount: TypeInformation[CardAccountElement] = TypeInformation.of(classOf[(CardAccountElement)])

    val config = new Configuration()
    config.setString("metrics.reporters", "consoleReporter")
    config.setString("metrics.reporter.consoleReporter.class", "io.flinkjobsexamples.metrics.ConsoleReporter")
    config.setString("metrics.reporter.consoleReporter.interval", "2 SECONDS")

    val parallelism = 1
    val numEvents = 100
    val sourcePause = 50
    val ordered = false
    val asyncTimeout = 100L

    val env =  new StreamExecutionEnvironment(new LocalStreamEnvironment(config))
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
