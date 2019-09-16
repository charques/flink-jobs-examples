package io.flinkjobsexamples.elements

class CardAccountElement(var iban: String, var amount: Int, var identifier: String) {

  def this() = {
    this("", 0, "")
  }

  override def toString = s"iban=$iban amount=$amount identifier=$identifier"
}
