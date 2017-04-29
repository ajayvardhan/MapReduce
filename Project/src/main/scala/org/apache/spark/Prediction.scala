package org.apache.spark

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.storage.StorageLevel

object Prediction {

  def main(args : Array[String]): Unit = {
    //Setup Spark
    val conf = new SparkConf().setAppName("Bird Classification").setMaster("yarn")
    val sc = new SparkContext(conf)

    // spark session to
    val spark = SparkSession
      .builder
      .appName("Bird Classification")
      .getOrCreate()

    //Setup model input and output paths
    val modelPath = args(0)
    val testInput = args(1)
    val output = args(2)

    //Partiotn 40 as 10 machines with 4 cores
    val testData = sc.textFile(testInput,40)
      .map(line => {
        val l = line.split(",")
        val id = l(0)
        var lbl = 0.0
        var vals : Array[Double] = Array()
        if(!l(2).equals("LATITUDE")){
          //slice out core vairates and checklist
          var values = l.slice(2, 7)
          values ++= l.slice(12,14)
          values ++= l.slice(16,16)
          values ++= l.slice(955,960)
          values ++= l.slice(962,1015)
          vals = values.map(x => {
            //replace unknwon values with 0
            if(x.equals("?")){
              0
            }
            else{
              x.toDouble
            }
          })
        }
        (id, LabeledPoint(lbl, Vectors.dense(vals)))
      }).filter(line => line._2.features.size>0)

    //fetch stored model
    val model = RandomForestModel.load(sc,modelPath)

    val head = Array(("SAMPLING_EVENT_ID", "SAW_AGELAIUS_PHOENICEUS"))

    //create RDD from tuple
    val header = sc.parallelize(head)

    //Predict for each row in label
    val labelAndPreds = testData.map { point =>
      val prediction = model.predict(point._2.features)
      (point._1, prediction.toString)
    }

    //add header to top
    val out = header.union(labelAndPreds)

    out.saveAsTextFile(output)

    spark.stop()
  }
}