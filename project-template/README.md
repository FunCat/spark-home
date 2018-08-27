To run the application, you need to run the following command:
```
    spark-submit --class com.epam.hubd.spark.scala.core.homework.MotelsHomeRecommendation spark-core-homework-1.0.0.jar integration/input/bids.txt integration/input/motels.txt integration/input/exchange_rate.txt /tmp/spark-core
```
You must ensure that the output directory does not exist and that the output folder is in the correct location.

 