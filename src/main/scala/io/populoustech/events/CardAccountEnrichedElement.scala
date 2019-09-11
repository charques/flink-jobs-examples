package io.populoustech.events

import java.util.Calendar

class CardAccountEnrichedElement(var iban: String, var amount: Int, var identifier:String) {

  val creationTime: Long = Calendar.getInstance().getTime.getTime

  def getCreationTime: Long = creationTime

  override def equals(obj: Any): Boolean = {
    obj match {
      case cardAccountElement: CardAccountEnrichedElement =>
        cardAccountElement.canEqual(this) &&
          iban == cardAccountElement.iban &&
          amount == cardAccountElement.amount &&
          identifier == cardAccountElement.identifier
      case _ => false
    }
  }

  def canEqual(obj: Any): Boolean = obj.isInstanceOf[CardAccountEnrichedElement]

  override def toString: String = "iban: " + iban + " - amount: " + amount + " - identifier: " + identifier
}
