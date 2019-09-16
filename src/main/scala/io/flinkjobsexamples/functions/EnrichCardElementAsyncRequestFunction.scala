package io.flinkjobsexamples.functions

import io.flinkjobsexamples.elements.{CardAccountElement, CardElement}
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}

import scala.concurrent.{ExecutionContext, Future}

class EnrichCardElementAsyncRequestFunction extends RichAsyncFunction[CardElement, CardAccountElement] {

  override def asyncInvoke(input: CardElement, resultFuture: ResultFuture[CardAccountElement]): Unit = {
    Future {
      val r = scala.util.Random
      val identifier = "%010d".format(r.nextInt(10000))
      resultFuture.complete(Seq(new CardAccountElement(input.iban, input.amount, identifier)))
    } (ExecutionContext.global)
  }

  override def timeout(input: CardElement, resultFuture: ResultFuture[CardAccountElement]): Unit = {
    val r = scala.util.Random
    val identifier = "%06d".format(r.nextInt(10000))
    resultFuture.complete(Seq(new CardAccountElement(input.iban, input.amount, identifier)))
  }
}