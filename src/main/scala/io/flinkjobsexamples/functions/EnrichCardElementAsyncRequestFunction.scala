package io.flinkjobsexamples.functions

import io.flinkjobsexamples.elements.{AccountElement, CardAccountElement, CardElement}
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.configuration.Configuration
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper
import org.apache.flink.metrics.Meter
import org.apache.flink.streaming.api.scala.async.{ResultFuture, RichAsyncFunction}

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

class EnrichCardElementAsyncRequestFunction extends RichAsyncFunction[CardElement, CardAccountElement] {

  implicit val typeInfoAccount: TypeInformation[AccountElement] = TypeInformation.of(classOf[AccountElement])

  var meter: Meter = _
  var ibanIdentifierMap: immutable.Map[String, String] = _

  override def open(parameters: Configuration): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    // init iban/identifier map
    val lines: DataSet[AccountElement] = env.readCsvFile("files/accounts_data.csv",
      "\n", ",", null, true,
      null, false, null, Array("iban", "identifier"))
    ibanIdentifierMap = lines.collect().map(a => (a.iban, a.identifier)).toList.toMap

    // meter
    val dropwizardMeter = new com.codahale.metrics.Meter()
    meter = getRuntimeContext.getMetricGroup.addGroup("EnrichCardElementAsyncRequestFunctionMetricsGroup")
      .meter("EnrichEventAsyncJobMeter", new DropwizardMeterWrapper(dropwizardMeter))
  }

  override def asyncInvoke(input: CardElement, resultFuture: ResultFuture[CardAccountElement]): Unit = {
    Future {
      meter.markEvent()
      resultFuture.complete(Seq(new CardAccountElement(input.iban, input.amount, ibanIdentifierMap(input.iban))))
    } (ExecutionContext.global)
  }

  override def timeout(input: CardElement, resultFuture: ResultFuture[CardAccountElement]): Unit = {
    meter.markEvent()
    resultFuture.complete(Seq(new CardAccountElement(input.iban, input.amount, ibanIdentifierMap(input.iban))))
  }

}