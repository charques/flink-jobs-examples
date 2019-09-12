package io.flinkjobsexamples.asyncdatastream.sources

import io.flinkjobsexamples.asyncdatastream.events.{CardAccountEnrichedElement, CardEvent}
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}

import scala.concurrent.{ExecutionContext, Future}

class EnrichCardEventAsyncRequestFunction extends RichAsyncFunction[CardEvent, CardAccountEnrichedElement] {

  override def asyncInvoke(input: CardEvent, resultFuture: ResultFuture[CardAccountEnrichedElement]): Unit = {
    Future {
      val r = scala.util.Random
      val identifier = "%010d".format(r.nextInt(10000))
      resultFuture.complete(Seq(new CardAccountEnrichedElement(input.iban, input.amount, identifier)))
    } (ExecutionContext.global)
  }

  override def timeout(input: CardEvent, resultFuture: ResultFuture[CardAccountEnrichedElement]): Unit = {
    val r = scala.util.Random
    val identifier = "%06d".format(r.nextInt(10000))
    resultFuture.complete(Seq(new CardAccountEnrichedElement(input.iban, input.amount, identifier)))
  }
}