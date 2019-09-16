package io.flinkjobsexamples.elements

class AccountElement(var iban: String, var identifier: String) {

  def this() = {
    this("", "")
  }

  override def toString = s"iban=$iban identifier=$identifier"

}
