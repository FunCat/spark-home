package com.epam.hubd.spark.scala.core.homework

import java.io.File
import java.nio.file.Files

import com.epam.hubd.spark.scala.core.homework.MotelsHomeRecommendation.{AGGREGATED_DIR, ERRONEOUS_DIR}
import com.epam.hubd.spark.scala.core.homework.domain.{BidItem, EnrichedItem}
import com.epam.hubd.spark.scala.core.util.RddComparator
import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext, SparkContextProvider}
import org.apache.hadoop.fs.Path
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.junit._
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FunSuite}

/**
  * Created by Csaba_Bejan on 8/17/2016.
  */
class MotelsHomeRecommendationTest extends FunSuite with SharedSparkContext with RDDComparisons with BeforeAndAfter
  with BeforeAndAfterAll with SparkContextProvider {

  override def conf = new SparkConf().setMaster("local[2]").setAppName("motels-home-recommendation test")

  val INPUT_BIDS_SAMPLE = "src/test/resources//bids_sample.txt"
  val INPUT_EXCHANGE_RATE_SAMPLE = "src/test/resources//exchange_rate_sample.txt"
  val INPUT_MOTELS_SAMPLE = "src/test/resources//motels_sample.txt"

  val INPUT_BIDS_INTEGRATION = "src/test/resources//integration/input/bids.txt"
  val INPUT_EXCHANGE_RATES_INTEGRATION = "src/test/resources//integration/input/exchange_rate.txt"
  val INPUT_MOTELS_INTEGRATION = "src/test/resources//integration/input/motels.txt"

  val EXPECTED_AGGREGATED_INTEGRATION = "src/test/resources/integration/expected_output/aggregated"
  val EXPECTED_ERRORS_INTEGRATION = "src/test/resources/integration/expected_output/error_records"

  private var outputFolder: File = null

  before {
    outputFolder = Files.createTempDirectory("output").toFile
  }

  test("should read raw bids") {
    val expected = sc.parallelize(
      Seq(
        List("0000002", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07", "", "1.35"),
        List("0000001", "06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL")
      )
    )

    val rawBids = MotelsHomeRecommendation.getRawBids(sc, INPUT_BIDS_SAMPLE)

    assertRDDEquals(expected, rawBids)
  }

  test("should collect erroneous records") {
    val rawBids = sc.parallelize(
      Seq(
        List("1", "06-05-02-2016", "ERROR_1"),
        List("2", "15-04-08-2016", "0.89"),
        List("3", "07-05-02-2016", "ERROR_2"),
        List("4", "06-05-02-2016", "ERROR_1"),
        List("5", "06-05-02-2016", "ERROR_2")
      )
    )

    val expected = sc.parallelize(
      Seq(
        "06-05-02-2016,ERROR_1,2",
        "06-05-02-2016,ERROR_2,1",
        "07-05-02-2016,ERROR_2,1"
      )
    )

    val erroneousRecords = MotelsHomeRecommendation.getErroneousRecords(rawBids)

    assertRDDEquals(expected, erroneousRecords)
  }


  test("should filter errors and create correct aggregates") {

    runIntegrationTest()

    //If the test fails and you are interested in what are the differences in the RDDs uncomment the corresponding line
    //printRddDifferences(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    //printRddDifferences(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))

    assertRddTextFiles(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
    assertAggregatedFiles(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
  }

  test("should read file with exchange rates") {
    val expected = Map(
      "11-05-08-2016" -> 0.873,
      "10-06-11-2015" -> 0.987,
      "10-05-02-2016" -> 0.876
    )

    val actual = MotelsHomeRecommendation.getExchangeRates(sc, INPUT_EXCHANGE_RATE_SAMPLE)
    assert(expected, actual)
  }

  test("should return bids splitted by countries") {
    val rawBids = sc.parallelize(
      Seq(
        List("0000002", "11-05-08-2016", "0.92", "1.68", "0.81", "0.68", "1.59", "", "1.63", "1.77", "2.06", "0.66", "1.53", "", "0.32", "0.88", "0.83", "1.01"),
        List("0000001", "22-04-08-2016", "ERROR_INCONSISTENT_DATA")
      )
    )
    val exchange_rates = Map(
      "11-05-08-2016" -> 0.873,
      "10-06-11-2015" -> 0.987,
      "10-05-02-2016" -> 0.876
    )

    val expected = sc.parallelize(
      Seq(
        BidItem("0000002", "2016-08-05 11:00", "CA", 1.423)
      )
    )

    val actual = MotelsHomeRecommendation.getBids(rawBids, exchange_rates)
    assertRDDEquals(expected, actual)
  }

  test("should read file with motels") {
    val expected = sc.parallelize(
      Seq(
        ("0000001", "Olinda Windsor Inn"),
        ("0000002", "Merlin Por Motel")
      )
    )

    val actual = MotelsHomeRecommendation.getMotels(sc, INPUT_MOTELS_SAMPLE)
    assertRDDEquals(expected, actual)
  }

  test("should return bids with motel name") {
    val expected = sc.parallelize(
      Seq(
        EnrichedItem("0000002", "Merlin Por Motel", "2016-08-05 11:00", "CA", 1.423),
        EnrichedItem("0000005", "Majestic Ibiza Por Hostel", "2016-09-04 18:00", "US", 1.154),
        EnrichedItem("0000003", "Olinda Big River Casino", "2015-11-07 04:00", "MX", 0.994)
      )
    )

    val bids = sc.parallelize(
      Seq(
        BidItem("0000002", "2016-08-05 11:00", "CA", 1.423),
        BidItem("0000005", "2016-09-04 18:00", "US", 1.154),
        BidItem("0000003", "2015-11-07 04:00", "MX", 0.994)
      )
    )

    val motels = sc.parallelize(
      Seq(
        ("0000001", "Olinda Windsor Inn"),
        ("0000002", "Merlin Por Motel"),
        ("0000003", "Olinda Big River Casino"),
        ("0000004", "Majestic Big River Elegance Plaza"),
        ("0000005", "Majestic Ibiza Por Hostel")
      )
    )

    val actual = MotelsHomeRecommendation.getEnriched(bids, motels)
    assertRDDEquals(expected, actual)
  }

  after {
    outputFolder.delete
  }

  private def runIntegrationTest() = {
    MotelsHomeRecommendation.processData(sc, INPUT_BIDS_INTEGRATION, INPUT_MOTELS_INTEGRATION, INPUT_EXCHANGE_RATES_INTEGRATION, outputFolder.getAbsolutePath)
  }

  private def assertRddTextFiles(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    assertRDDEquals(expected, actual)
  }

  private def assertAggregatedFiles(expectedPath: String, actualPath: String) = {
    val expected = parseLastDouble(sc.textFile(expectedPath)).collect.toMap
    val actual = parseLastDouble(sc.textFile(actualPath)).collect.toMap
    if (expected.size != actual.size) {
      Assert.fail(s"Aggregated have wrong number of records (${actual.size} instead of ${expected.size})")
    }
    expected.foreach(x => {
      val key = x._1
      val expectedValue = x._2
      if (!actual.contains(key)) {
        Assert.fail(s"Aggregated does not contain: $key,$expectedValue")
      }
      val actualValue = actual(key)
      if (Math.abs(expectedValue - actualValue) > 0.0011) {
        Assert.fail(s"Aggregated have different value for: $key ($actualValue instead of $expectedValue)")
      }
    })
  }
  
  private def parseLastDouble(rdd: RDD[String]) = {
    rdd.map(s => {
      val commaIndex = s.lastIndexOf(",")
      (s.substring(0, commaIndex), s.substring(commaIndex + 1).toDouble)
    })
  }

  private def printRddDifferences(expectedPath: String, actualPath: String) = {
    val expected = sc.textFile(expectedPath)
    val actual = sc.textFile(actualPath)
    RddComparator.printDiff(expected, actual)
  }

  private def getOutputPath(dir: String): String = {
    new Path(outputFolder.getAbsolutePath, dir).toString
  }
}
