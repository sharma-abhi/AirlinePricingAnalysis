import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer, VectorAssembler}
import org.apache.spark.sql.functions._
/*
 * <h1>Spark Model</h1>
 * The SparkModel program runs a Random Forest Model on the sanitized Flights history data
 *
 * @author: Abhi, Afan, Deepen and Akshay
 * @version: 2.0
 * @since: March 5, 2016
 */
object SparkModel{ 
    // DataFrame Scheme for Training, Test and Validation dataset.
    // (required to be mentioned outside of main method)
    case class Flight(popularOrigin: Double, layover: Double, dayOfWeek:Double, month:Double, crsTime: Double, distanceGroup:Double, label:Double)
    case class FlightTest(keyYear: Double, keyMonth: Double, keyDay: Double, keyOrigin: String, keyDest: String, Flight1: Double, Flight2: Double, popularOrigin: Double, layover: Double, dayOfWeek:Double, month:Double, crsTime: Double, distanceGroup:Double, duration:Double)
    //case class FlightRequest(keyYear: Double, keyMonth: Double, keyDay: Double, keyOrigin: String, keyDest: String)
    /**
    * Main Function of object.
    * Trains a Model on given Train File, 
    * joins Test File with Validation File to fetch labels,
    * Predicts Labels on Test Data and
    * Outputs folders containing Feature Importance, Test Predictions and 
    *  Precision, Recall and F1-Score Metrics.
    * @param args TrainingFilePath, TestFilePath, ValidationFilePath, 
    *             FeatureImportanceOutputPath, PredictionOutputPath, MetricsOutputPath
    * @return Feature Importances, Test Predictions, Metrics
    */
    def main(args: Array[String]) {
        // Setting up configurations
        val conf = new SparkConf().
            setAppName("SparkModel")
        val sc = new SparkContext(conf)  
        val sqlContext = new org.apache.spark.sql.SQLContext(sc)        
        import sqlContext.implicits._ // Import required to be called only after above statement
        // Initializing parameters
        val seed: Long = 42 // Setting seed for model reproducibility
        val nTrees = 10
        val trainFile = args(0); //val trainFile = "/home/abhijeet/Sharma_Khan_A7/output/a7history/final-clean-train-data"
        val testFile = args(1); //val testFile = "/home/abhijeet/Sharma_Khan_A7/output/a7test/final-clean-test-data"
        //val requestFile = args(2); //val requestFile = "/home/abhijeet/Sharma_Khan_A7/input/a7request/04req10k.csv.gz"
        val featureImportancesPath = args(2);   //val featureImportancesPath =  "/home/abhijeet/Sharma_Khan_A7/output/feature_importances"
        val testPredictionsPath = args(3);      //val testPredictionsPath =  "/home/abhijeet/Sharma_Khan_A7/output/testPredictions"
        // Creating train dataframe
        val inputDf = sc.textFile(trainFile).
            map(_.split("\t")).
            map(f => Flight(f(1).toDouble, f(2).toDouble, f(3).toDouble, 
                f(4).toDouble, f(5).toDouble, f(6).toDouble, f(7).toDouble)).toDF()
        val zeroLabelDf = inputDf.filter(inputDf("label") === 0.0)    
        val oneLabelDf = inputDf.filter(inputDf("label") === 1.0)    
        val sampledZeroDf = zeroLabelDf.sample(false, 0.2, seed)
        val combinedDf = sampledZeroDf.unionAll(oneLabelDf)
        // Assembling feature columns as "features"
        val assembler = new VectorAssembler().
            setInputCols(Array("popularOrigin", "layover", 
                                "dayOfWeek", "month", "crsTime", "distanceGroup")).
            setOutputCol("features")
        val trainingData = assembler.transform(combinedDf)
        // Indexing labels, adding metadata to the label column.
        val labelIndexer = new StringIndexer().
            setInputCol("label").
            setOutputCol("indexedLabel").
            fit(trainingData)
        // Automatically identify categorical features, and index them.
        val featureIndexer = new VectorIndexer().
            setInputCol("features").
            setOutputCol("indexedFeatures").
            setMaxCategories(4).            // features with > 4 distinct values are treated as continuous
            fit(trainingData) 
        // Setting up Random Forest Model
        val rf  = new RandomForestClassifier().
            setFeaturesCol("features").
            setLabelCol(labelIndexer.getOutputCol).
            //setLabelCol("label").
            setNumTrees(nTrees).
            setFeatureSubsetStrategy("auto"). //letting model figure out optimal strategy
            setImpurity("gini").
            setMaxDepth(4).
            setMaxBins(4).
            setSeed(seed)
        // Convert indexed labels back to original labels.
        val labelConverter = new IndexToString().
            setInputCol("prediction").
            setOutputCol("predictedLabel").
            setLabels(labelIndexer.labels)

        // Chain indexers and forest in a Pipeline
        val pipeline = new Pipeline().
            setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))
        // Running the pipeline.
        val model = pipeline.fit(trainingData)
        // Output Feature Importances to File(s)
        val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
        sc.parallelize(rfModel.
                        featureImportances.toArray, 1). //Parellizing to one reducer to avoid multiple output files
                        saveAsTextFile(featureImportancesPath)
        // Creating test dataframe
        val testDf = sc.textFile(testFile).
                        map(_.split("\t")).
                        map(f => FlightTest(f(0).toDouble, f(1).toDouble, f(2).toDouble, f(3), f(4),
                            f(5).toDouble, f(6).toDouble, f(7).toDouble, f(8).toDouble, f(9).toDouble, 
                            f(10).toDouble, f(11).toDouble, f(12).toDouble, f(13).toDouble)).toDF()
       
        // Predict on Testing data
        val testingData = assembler.transform(testDf)
        val featureIndexedTestingDf = featureIndexer.transform(testingData)
        val testPredictions = model.transform(featureIndexedTestingDf)
        testPredictions.select("keyYear", "keyMonth", "keyDay", "keyOrigin", "keyDest", "Flight1", "Flight2", "duration", "predictedLabel").
                        rdd.        // convert from Dataframe to RDD for saving as Text File
                        saveAsTextFile(testPredictionsPath) // Save Predictons        
        sc.stop()   //stop spark context        
    }
}
