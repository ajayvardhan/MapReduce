package org.apache.spark

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.apache.spark.storage.StorageLevel

object App {


  def main(args : Array[String]): Unit = {
    //Initalize Spark
    val conf = new SparkConf().setAppName("Bird Classification").setMaster("yarn")
    val sc = new SparkContext(conf)

    // spark session to
    val spark = SparkSession
      .builder
      .appName("Bird Classification")
      .getOrCreate()
    // Set input paths
    val trainingPath = args(0)
    //set model output path
    val modelPath = args(1)

    //Method to create LabeledPoint for each line in input
    def getLabeledPoint(line: String): LabeledPoint ={
      //Delimter for line is ,
      val l = line.split(",")
      var lbl = 0.0
      var vals : Array[Double] = Array()
      //Do not consider header line & do not consider lines without primaryFlag
      if(!l(2).equals("LATITUDE")){
        //  AGELAIUS_PHOENICEUS counts column
        val label = l(26)
        //X value is present if bird is seen but count is unknown
        if(label.equals("X")){
          lbl = 1.0
        }
        else{
          //Convert labels for counts greater than 1 to 1
          if(label.toDouble > 1.0){
            lbl = 1.0
          }
        }
        //speerate checklist columns
        var values = l.slice(2, 7)
        values ++= l.slice(12,14)
        values ++= l.slice(16,16)
        //use core covarites without using sampling id and location id
        values ++= l.slice(955,960)
        values ++= l.slice(962,1015)
        vals = values.map(x => {
          //Replace ? with 0 for unknonwn values
          if(x.equals("?")){
            0
          }
          else{
            x.toDouble
          }
        })
      }
      LabeledPoint(lbl, Vectors.dense(vals))
    }


    //Convert input file to RDD[LabeledPoint]
    val labeledData = sc.textFile(trainingPath,40)
      .map(line => getLabeledPoint(line)).filter(line => line.features.size>0).persist(StorageLevel.MEMORY_AND_DISK)


   //SPlit inputData into training and validation Data
    val Array(trainingData,validationData) = labeledData.randomSplit(Array(0.8,0.2))

    //Classes are set to 2 (0,1)
    val numClasses = 2
    //Num trees set to 40
    val numTrees = 40
    //features strategy set to auto , will reduce features
    val featureSubsetStrategy = "auto"
    //loss calculated using gini
    val impurity = "gini"
    //depth set to 17
    val maxDepth = 17
    //maximum values possible for categorical and continous features
    val maxBins = 125

    // years, bcr ,bailey_eco region
    val categoricalFeaturesInfo= Map[Int, Int]((3,13),(15,38),(16,121))

    //Train Random Forest model using the paramters sepecified above
    val model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)

    //save Model to path.
    model.save(sc,modelPath)

    //Predict for validation data
    val predictionVlidation = validationData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }


    // Calcualte accuracy, if labels are same as predicted then the label is correct
    val accuracy = predictionVlidation.filter(r => r._1 == r._2).count.toDouble / validationData.count()

    println("Accuracy = " + accuracy.toString)

    spark.stop()
  }

}