package com.epam.hubd.spark.scala.core

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext, SparkContextProvider}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

class SparkTransformations extends FunSuite with SharedSparkContext with RDDComparisons with BeforeAndAfter
  with BeforeAndAfterAll with SparkContextProvider  {

  override def conf = new SparkConf().setMaster("local[2]").setAppName("test")

  val INPUT_USERS = "/test/users.txt"
  val INPUT_SHOPPING_LIST = "/test/shopping_list.txt"
  val INPUT_PRICE_LIST = "/test/price_list.txt"

  test("first") {
    val expected = "1,Rene Morgan,rene.morgan81@example.com,08-05-1972"
    val actual = readFile(sc, INPUT_USERS).first()
    assert(expected, actual)
  }

  test("map") {
    val expected = sc.parallelize(
      Seq(
        List("1", "Rene Morgan", "rene.morgan81@example.com", "08-05-1972"),
        List("2", "Kevin Evans", "kevin.evans67@example.com", "01-03-1973"),
        List("3", "Jeffery Brown", "jeffery.brown42@example.com", "09-07-1979")
      )
    )

    val actual = readFile(sc, INPUT_USERS).map(s => s.split(",").toList)
    assertRDDEquals(expected, actual)
  }

  test("flatMap") {
    val expected = sc.parallelize(
      Seq(
        Item("1", "pen", 1, 23),
        Item("1", "pencil", 1, 28),
        Item("1", "notepad", 1, 143),
        Item("1", "paper", 1, 204),
        Item("1", "eraser", 1, 45),
        Item("2", "paper folder", 1, 48),
        Item("1", "pen", 1, 23),
        Item("1", "pen", 1, 23),
        Item("1", "pen", 1, 23),
        Item("1", "color pencils", 1, 347),
        Item("1", "paper", 1, 204),
        Item("1", "paper folder", 1, 48),
        Item("1", "paper folder", 1, 48),
        Item("1", "eraser", 1, 45),
        Item("1", "pencil", 1, 28),
        Item("1", "pencil", 1, 28),
        Item("3", "pen", 1, 23),
        Item("3", "pen", 1, 23),
        Item("3", "pencil", 1, 28),
        Item("3", "pencil", 1, 28),
        Item("1", "pen", 1, 23)
      )
    )

    val shopping_list = readFile(sc, INPUT_SHOPPING_LIST).map(s => s.split(",").toList)
    val price_list = readFile(sc, INPUT_PRICE_LIST).map(s => (s.split(",")(0), s.split(",")(1).toInt)).collect().toMap

    val actual = shopping_list.flatMap(s => s(3).split(";").map(c => Item(s(0), c, 1, price_list(c))))
    assertRDDEquals(expected, actual)
  }

  test("filter") {
    val expected = sc.parallelize(
      Seq(
        List("1", "5", "443", "pen;pencil;notepad;paper;eraser"),
        List("1", "10", "817", "pen;pen;pen;color pencils;paper;paper folder;paper folder;eraser;pencil;pencil"),
        List("3", "4", "102", "pen;pen;pencil;pencil")
      )
    )

    val actual = readFile(sc, INPUT_SHOPPING_LIST).map(s => s.split(",").toList).filter(s => s(2).toInt > 100)
    assertRDDEquals(expected, actual)
  }

  test("distinct") {
    val expected = sc.parallelize(
      Seq(
        Item("1", "pen", 1, 23),
        Item("1", "pencil", 1, 28),
        Item("1", "notepad", 1, 143),
        Item("1", "paper", 1, 204),
        Item("1", "eraser", 1, 45),
        Item("2", "paper folder", 1, 48),
        Item("1", "color pencils", 1, 347),
        Item("1", "paper folder", 1, 48),
        Item("3", "pen", 1, 23),
        Item("3", "pencil", 1, 28)
      )
    )

    val shopping_list = readFile(sc, INPUT_SHOPPING_LIST).map(s => s.split(",").toList)
    val price_list = readFile(sc, INPUT_PRICE_LIST).map(s => (s.split(",")(0), s.split(",")(1).toInt)).collect().toMap

    val actual = shopping_list.flatMap(s => s(3).split(";").map(c => Item(s(0), c, 1, price_list(c)))).distinct()
    assertRDDEquals(expected, actual)
  }

  test("groupByKey") {
    val expected = sc.parallelize(
      Seq(
        ("eraser", Iterable(Item("1", "eraser", 1, 45), Item("1", "eraser", 1, 45))),
        ("paper", Iterable(Item("1", "paper", 1, 204), Item("1", "paper", 1, 204))),
        ("paper folder", Iterable(Item("2", "paper folder", 1, 48), Item("1", "paper folder", 1, 48), Item("1", "paper folder", 1, 48))),
        ("pen", Iterable(Item("1", "pen", 1, 23), Item("1", "pen", 1, 23), Item("1", "pen", 1, 23), Item("1", "pen", 1, 23), Item("3", "pen", 1, 23), Item("3", "pen", 1, 23), Item("1", "pen", 1, 23))),
        ("color pencils", Iterable(Item("1", "color pencils", 1, 347))),
        ("notepad", Iterable(Item("1", "notepad", 1, 143))),
        ("pencil", Iterable(Item("1", "pencil", 1, 28), Item("1", "pencil", 1, 28), Item("1", "pencil", 1, 28), Item("3", "pencil", 1, 28), Item("3", "pencil", 1, 28)))
      )
    )

    val shopping_list = readFile(sc, INPUT_SHOPPING_LIST).map(s => s.split(",").toList)
    val price_list = readFile(sc, INPUT_PRICE_LIST).map(s => (s.split(",")(0), s.split(",")(1).toInt)).collect().toMap

    val actual = shopping_list.flatMap(s => s(3).split(";").map(c => (c, Item(s(0), c, 1, price_list(c))))).groupByKey()
    assertRDDEquals(expected, actual)
  }

  test("reduceByKey") {
    val expected = sc.parallelize(
      Seq(
        Item("1", "eraser", 2, 90),
        Item("1", "paper", 2, 408),
        Item("2", "paper folder", 3, 144),
        Item("1", "pen", 7, 161),
        Item("1", "color pencils", 1, 347),
        Item("1", "notepad", 1, 143),
        Item("1", "pencil", 5, 140)
      )
    )

    val shopping_list = readFile(sc, INPUT_SHOPPING_LIST).map(s => s.split(",").toList)
    val price_list = readFile(sc, INPUT_PRICE_LIST).map(s => (s.split(",")(0), s.split(",")(1).toInt)).collect().toMap

    val actual = shopping_list.flatMap(s => s(3).split(";").map(c => (c, Item(s(0), c, 1, price_list(c))))).reduceByKey((s1, s2) => Item(s1.userId, s1.name, s1.count + s2.count, s1.price)).map(s => Item(s._2.userId, s._2.name, s._2.count, s._2.price * s._2.count))
    assertRDDEquals(expected, actual)
  }

  def readFile(sc: SparkContext, filePath: String): RDD[String] = {
    val path = getClass.getResource(filePath)
    sc.textFile(path.toString)
  }
}
