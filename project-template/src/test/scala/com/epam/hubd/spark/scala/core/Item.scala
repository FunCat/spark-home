package com.epam.hubd.spark.scala.core

case class Item(userId: String, name: String, count: Int, price: Int) {

  override def toString: String = s"$userId,$name,$count,$price"
}