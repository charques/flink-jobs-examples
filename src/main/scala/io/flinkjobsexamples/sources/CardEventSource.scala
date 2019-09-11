package io.flinkjobsexamples.sources

import io.flinkjobsexamples.events.CardEvent
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}
import org.slf4j.{Logger, LoggerFactory}

class CardEventSource(val pause: Long) extends RichParallelSourceFunction[CardEvent] {

  val logger: Logger = LoggerFactory.getLogger(this.getClass)

  private var running = true
  private var counter = 0

  override def run(sourceContext: SourceFunction.SourceContext[CardEvent]): Unit = {
    while ( {
      counter < 100
    }) {
      val iban = "%024d".format(counter)
      val r = scala.util.Random
      val amount = r.nextInt(5000)

      val cardEvent = new CardEvent(iban, amount)
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
