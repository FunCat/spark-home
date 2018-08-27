package com.epam.hubd.spark.scala.core.homework

import java.text.SimpleDateFormat

import com.epam.hubd.spark.scala.core.homework.domain.{BidError, BidItem, EnrichedItem}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val sc = new SparkContext(new SparkConf().setAppName("motels-home-recommendation").setMaster("local[1]"))

    processData(sc, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(sc: SparkContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      * Split each line from the file and return list of strings.
      */
    val rawBids: RDD[List[String]] = getRawBids(sc, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      * Find all records which contain ERROR message and after that group them by date.
      */
    val erroneousRecords: RDD[String] = getErroneousRecords(rawBids)
    erroneousRecords.saveAsTextFile(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Split each line and return map where key - date and value - exchange rate.
      */
    val exchangeRates: Map[String, Double] = getExchangeRates(sc, exchangeRatesPath)

    /**
      * Task 3:
      * Transform the rawBids and use the BidItem case class.
      * Get all valid records, convert each record to three records based on the interested countries, and return only one record with the highest price.
      */
    val bids: RDD[BidItem] = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Split each line and return key/value RDD, where key - motelId and value - motel name.
      */
    val motels: RDD[(String, String)] = getMotels(sc, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names and utilize EnrichedItem case class.
      */
    val enriched: RDD[EnrichedItem] = getEnriched(bids, motels)
    enriched.saveAsTextFile(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sc: SparkContext, bidsPath: String): RDD[List[String]] = {
    sc.textFile(bidsPath).map(s => s.split(",").toList)
  }

  def getErroneousRecords(rawBids: RDD[List[String]]): RDD[String] = {
    rawBids.filter(line => line(2).contains("ERROR_")).map(line => BidError(line(1), line(2)).toString).groupBy(s => s).map(s => s._1 + "," + s._2.size)
  }

  def getExchangeRates(sc: SparkContext, exchangeRatesPath: String): Map[String, Double] = {
    sc.textFile(exchangeRatesPath).map(s => (s.split(",")(0), s.split(",")(3).toDouble)).collect().toMap
  }

  def getBids(rawBids: RDD[List[String]], exchangeRates: Map[String, Double]): RDD[BidItem] = {
    val correctBidsMap = rawBids.filter(line => !line(2).contains("ERROR_")).filter(line => !(line(5).isEmpty && line(6).isEmpty && line(8).isEmpty))
    correctBidsMap.map(s => List(
      BidItem(s(0), Constants.OUTPUT_DATE_FORMAT.print(Constants.INPUT_DATE_FORMAT.parseDateTime(s(1))), "US", rounded(if(s(5).isEmpty) 0 else s(5).toDouble * exchangeRates(s(1)), 3)),
      BidItem(s(0), Constants.OUTPUT_DATE_FORMAT.print(Constants.INPUT_DATE_FORMAT.parseDateTime(s(1))), "CA", rounded(if(s(8).isEmpty) 0 else s(8).toDouble * exchangeRates(s(1)), 3)),
      BidItem(s(0), Constants.OUTPUT_DATE_FORMAT.print(Constants.INPUT_DATE_FORMAT.parseDateTime(s(1))), "MX", rounded(if(s(6).isEmpty) 0 else s(6).toDouble * exchangeRates(s(1)), 3))
    )).map(s => getMaxBidItem(s))
  }

  def getMotels(sc: SparkContext, motelsPath: String): RDD[(String, String)] = {
    sc.textFile(motelsPath).map(s => (s.split(",")(0), s.split(",")(1)))
  }

  def getEnriched(bids: RDD[BidItem], motels: RDD[(String, String)]): RDD[EnrichedItem] = {
    val newBids = bids.map(s => (s.motelId, s))
    newBids.join(motels).map(s => EnrichedItem(s._1, s._2._2, s._2._1.bidDate, s._2._1.loSa, s._2._1.price))
  }

  def rounded(n: Double, x: Int) = {
    val w = Math.pow(10, x)
    math.round(n * w) / w
  }

  def getMaxBidItem(list: List[BidItem]) = {
    var maxPrice = 0.0
    var result = list(0)
    list.foreach(s => if(s.price > maxPrice){
      result = s
      maxPrice = s.price
    })
    result
  }
}
