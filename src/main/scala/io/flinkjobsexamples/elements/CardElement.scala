package io.flinkjobsexamples.elements

class CardElement(var iban: String, var amount: Int) {

  def this() = {
    this("", 0)
  }

  override def toString = s"iban=$iban amount=$amount"
}
