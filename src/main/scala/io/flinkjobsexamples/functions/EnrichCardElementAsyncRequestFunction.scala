package io.flinkjobsexamples.functions

import io.flinkjobsexamples.elements.{CardAccountElement, CardElement}
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

class EnrichCardElementAsyncRequestFunction extends RichAsyncFunction[CardElement, CardAccountElement] {

  // keep track of the already generated Identifiers by Iban
  // It would only work with parallelism = 1
  val ibanIdentifierMap: mutable.HashMap[String, String] = mutable.HashMap()

  override def asyncInvoke(input: CardElement, resultFuture: ResultFuture[CardAccountElement]): Unit = {
    Future {
      // TODO retrieve identifier via file
      val identifier = getIdentifier(input.iban)
      resultFuture.complete(Seq(new CardAccountElement(input.iban, input.amount, identifier)))
    } (ExecutionContext.global)
  }

  override def timeout(input: CardElement, resultFuture: ResultFuture[CardAccountElement]): Unit = {
    val identifier = getIdentifier(input.iban)
    resultFuture.complete(Seq(new CardAccountElement(input.iban, input.amount, identifier)))
  }

  private def getIdentifier(iban: String): String = {
    if(!ibanIdentifierMap.contains(iban)) {
      val r = scala.util.Random
      ibanIdentifierMap(iban) = "%010d".format(r.nextInt(10000))
    }
    ibanIdentifierMap(iban)
  }
}