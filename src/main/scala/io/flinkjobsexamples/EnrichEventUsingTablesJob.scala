package io.flinkjobsexamples

import io.flinkjobsexamples.elements.{AccountElement, CardAccountElement, CardElement}
import io.flinkjobsexamples.sources.CardElementSource
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._
import org.apache.flink.table.sources.CsvTableSource
import org.slf4j.{Logger, LoggerFactory}

object EnrichEventUsingTablesJob {

  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(this.getClass)
    implicit val typeInfoAccount: TypeInformation[AccountElement] = TypeInformation.of(classOf[AccountElement])
    implicit val typeInfoCard: TypeInformation[CardElement] = TypeInformation.of(classOf[CardElement])
    implicit val typeInfoCardAccount: TypeInformation[CardAccountElement] = TypeInformation.of(classOf[CardAccountElement])

    val numEvents = 4
    val sourcePause = 100

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    // configure table source for accounts: iban/identifier
    val accountsSource: CsvTableSource = CsvTableSource.builder()
      .path("files/accounts_data.csv")
      .ignoreFirstLine()
      .fieldDelimiter(",")
      .field("iban", Types.STRING)
      .field("identifier", Types.STRING)
      .build()

    // name table source for accounts
    tEnv.registerTableSource("accounts", accountsSource)

    // configure cards stream
    val cardsEventStream: DataStream[CardElement] = env.addSource(new CardElementSource(numEvents, sourcePause))
    // register the stream in the table environment
    tEnv.registerDataStream("cardsEvents", cardsEventStream)

    // define the accounts table
    val accountsTable: Table = tEnv
      .scan("accounts")
      .select('iban, 'identifier)

    // define the cards table
    val cardsTable: Table = tEnv
      .scan("cardsEvents")
      .select('iban, 'amount)

    // query using both tables
    val result: Table = tEnv.sqlQuery(s"SELECT $accountsTable.iban, $cardsTable.amount, $accountsTable.identifier FROM $accountsTable " +
      s"INNER JOIN $cardsTable ON $accountsTable.iban = $cardsTable.iban")

    // transfor the table in a stream
    val cardsAccount: DataStream[CardAccountElement] = result.toAppendStream[CardAccountElement]

    // sink
    cardsAccount.addSink((cardAccountElement: CardAccountElement) => {
      logger.info("cardAccountEnrichedElement: " + cardAccountElement.toString)
    })

    env.execute()
  }

}
