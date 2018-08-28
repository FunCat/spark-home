package com.epam.hubd.spark.scala.core.homework

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
      * Example:
      * input -> "0000002,15-04-08-2016,0.89,0.92,1.32,2.07,1.35"
      * output -> List("0000002", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07", "1.35")
      */
    val rawBids: RDD[List[String]] = getRawBids(sc, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      * Find all records which contain ERROR message and after that group them by date.
      * Example:
      * input -> List("1", "06-05-02-2016", "ERROR_1"),List("3", "07-05-02-2016", "ERROR_2"),List("4", "06-05-02-2016", "ERROR_1")
      * output -> "06-05-02-2016,ERROR_1,2","07-05-02-2016,ERROR_2,1"
      */
    val erroneousRecords: RDD[String] = getErroneousRecords(rawBids)
    erroneousRecords.saveAsTextFile(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Split each line and return map where key - date and value - exchange rate.
      * Example:
      * input -> "11-05-08-2016,Euro,EUR,0.873","10-06-11-2015,Euro,EUR,0.987","10-05-02-2016,Euro,EUR,0.876"
      * output -> Map("11-05-08-2016" -> 0.873,"10-06-11-2015" -> 0.987,"10-05-02-2016" -> 0.876)
      */
    val exchangeRates: Map[String, Double] = getExchangeRates(sc, exchangeRatesPath)

    /**
      * Task 3:
      * Transform the rawBids and use the BidItem case class.
      * Get all valid records, convert each record to three records based on the interested countries,
      * and return only one record with the highest price.
      * Example:
      * input rawBids -> List("0000002", "11-05-08-2016", "0.92", "1.68", "0.81", "0.68", "1.59", "", "1.63", "1.77", "2.06", "0.66", "1.53", "", "0.32", "0.88", "0.83", "1.01")
      * input exchangeRates -> Map("11-05-08-2016" -> 0.873,"10-06-11-2015" -> 0.987,"10-05-02-2016" -> 0.876)
      * output -> BidItem("0000002", "2016-08-05 11:00", "CA", 1.423)
      */
    val bids: RDD[BidItem] = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Split each line and return key/value RDD, where key - motelId and value - motel name.
      * Example:
      * input ->  "0000001,Olinda Windsor Inn,IN,http://www.motels.home/?partnerID=3cc6e91b-a2e0-4df4-b41c-766679c3fa28,Description",
      *           "0000002,Merlin Por Motel,JP,http://www.motels.home/?partnerID=4ba51050-eff2-458a-93dd-6360c9d94b63,Description"
      * output -> ("0000001", "Olinda Windsor Inn"),("0000002", "Merlin Por Motel")
      */
    val motels: RDD[(String, String)] = getMotels(sc, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names and utilize EnrichedItem case class.
      * Example:
      * input bids -> BidItem("0000002", "2016-08-05 11:00", "CA", 1.423), BidItem("0000003", "2015-11-07 04:00", "MX", 0.994)
      * input motels -> ("0000001", "Olinda Windsor Inn"),("0000002", "Merlin Por Motel"),("0000003", "Olinda Big River Casino")
      * output -> EnrichedItem("0000002", "Merlin Por Motel", "2016-08-05 11:00", "CA", 1.423),
      *           EnrichedItem("0000003", "Olinda Big River Casino", "2015-11-07 04:00", "MX", 0.994)
      */
    val enriched: RDD[EnrichedItem] = getEnriched(bids, motels)
    enriched.saveAsTextFile(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sc: SparkContext, bidsPath: String): RDD[List[String]] = {
    sc.textFile(bidsPath)
      .map(s => s.split(Constants.DELIMITER).toList)
  }

  def getErroneousRecords(rawBids: RDD[List[String]]): RDD[String] = {
    rawBids.filter(line => line(2).contains("ERROR_"))
      .map(line => BidError(line(1), line(2)).toString)
      .groupBy(s => s)
      .map(s => s._1 + Constants.DELIMITER + s._2.size)
  }

  def getExchangeRates(sc: SparkContext, exchangeRatesPath: String): Map[String, Double] = {
    sc.textFile(exchangeRatesPath)
      .map(s => (s.split(Constants.DELIMITER)(0), s.split(Constants.DELIMITER)(3).toDouble))
      .collect().toMap
  }

  def getBids(rawBids: RDD[List[String]], exchangeRates: Map[String, Double]): RDD[BidItem] = {
    val correctBidsMap = rawBids.filter(line => !line(2).contains("ERROR_"))
      .filter(line => !(line(5).isEmpty && line(6).isEmpty && line(8).isEmpty))

    correctBidsMap.map(s => List(
      getBidItem(s(0), s(1), "US", s(5), exchangeRates(s(1))),
      getBidItem(s(0), s(1), "CA", s(8), exchangeRates(s(1))),
      getBidItem(s(0), s(1), "MX", s(6), exchangeRates(s(1)))
    )).map(s => getMaxBidItem(s))
  }

  def getMotels(sc: SparkContext, motelsPath: String): RDD[(String, String)] = {
    sc.textFile(motelsPath)
      .map(s => (s.split(Constants.DELIMITER)(0), s.split(Constants.DELIMITER)(1)))
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
    list.foreach(s => if (s.price > maxPrice) {
      result = s
      maxPrice = s.price
    })
    result
  }

  def getBidItem(motelId: String, bidDate: String, loSa: String, price: String, exchangeRate: Double) = {
    BidItem(motelId, Constants.OUTPUT_DATE_FORMAT.print(Constants.INPUT_DATE_FORMAT.parseDateTime(bidDate)),
      loSa, rounded(if (price.isEmpty) 0 else price.toDouble * exchangeRate, 3))
  }
}
