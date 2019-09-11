package io.populoustech.events

import java.util.Calendar

class CardEvent(var iban: String, var amount: Int) {

  val creationTime: Long = Calendar.getInstance().getTime.getTime

  def getCreationTime: Long = creationTime

  override def equals(obj: Any): Boolean = {
    obj match {
      case cardEvent: CardEvent =>
        cardEvent.canEqual(this) &&
          iban == cardEvent.iban &&
          amount == cardEvent.amount
      case _ => false
    }
  }

  def canEqual(obj: Any): Boolean = obj.isInstanceOf[CardEvent]

  override def toString: String = "iban: " + iban + " - amount: " + amount
}
