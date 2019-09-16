package io.flinkjobsexamples.sources

import io.flinkjobsexamples.elements.CardElement
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.slf4j.{Logger, LoggerFactory}

class CardElementSource(val numEvent: Int, val pause: Long) extends RichSourceFunction[CardElement] {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private var running = true
  private var counter = 0

  override def run(sourceContext: SourceFunction.SourceContext[CardElement]): Unit = {
    while ( {
      counter < numEvent
    }) {
      val iban = "%024d".format(counter)
      val r = scala.util.Random
      val amount = r.nextInt(5000)

      val cardEvent = new CardElement(iban, amount)
      logger.info("cardEvent: " + cardEvent.toString)

      sourceContext.collect(cardEvent)

      counter = counter + 1
      Thread.sleep(pause)
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}
