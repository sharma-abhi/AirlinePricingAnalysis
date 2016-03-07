import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.udf
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.classification.{GBTClassificationModel, GBTClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorIndexer, VectorAssembler}
/*
 * <h1>Spark Model</h1>
 * The SparkModel program runs a Random Forest Model on the sanitized Flights history data
 *
 * @author: Sharma,Abhi and Khan,Afan Ahmad
 * @version: 1.0
 * @since: March 5, 2016
 */
object SparkModel{ 
    // DataFrame Scheme for Training, Test and Validation dataset.
    // (required to be mentioned outside of main method)
    case class Flight(carrier: String, year: Double, quarter:Double, month:Double, week:Double, dayOfMonth:Double, popularOrigin: Double, popularDest: Double, crsDepGroup: Double, crsArrGroup:Double, crsElapsed:Double, distanceGroup:Double, label:Double)
    case class FlightTest(carrier: String, id: String, year: Double, quarter:Double, month:Double, week:Double, dayOfMonth:Double, popularOrigin: Double, popularDest: Double, crsDepGroup: Double, crsArrGroup:Double, crsElapsed:Double, distanceGroup:Double)
    case class FlightVal(id: String, logicalLabel: String)//label:Double)
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
        val trainFile = args(0);
        val testFile = args(1);
        val validateFile = args(2);
        val featureImportancesPath = args(3);
        val testPredictionsPath = args(4);
        val metricsPath = args(5);
        // Creating train dataframe
        val inputDf = sc.textFile(trainFile).
            map(_.split("\t")).
            map(f => Flight(f(0), f(1).toDouble, f(2).toDouble, f(3).toDouble, 
                f(4).toDouble, f(5).toDouble, f(6).toDouble, f(7).toDouble, 
                f(8).toDouble, f(9).toDouble, f(10).toDouble, f(11).toDouble, 
                f(12).toDouble)).
            toDF()
        // Assembling feature columns as "features"
        val assembler = new VectorAssembler().
            setInputCols(Array("quarter", "month", "week", "dayOfMonth", 
                "popularOrigin", "popularDest", "crsDepGroup", "crsArrGroup", 
                "crsElapsed", "distanceGroup")).
            setOutputCol("features")
        val trainingData = assembler.transform(inputDf)
        // Indexing labels, adding metadata to the label column.
        val labelIndexer = new StringIndexer().
            setInputCol("label").
            setOutputCol("indexedLabel").
            fit(trainingData)
        // Automatically identify categorical features, and index them.
        val featureIndexer = new VectorIndexer().
            setInputCol("features").
            setOutputCol("indexedFeatures").
            setMaxCategories(5).            // features with > 5 distinct values are treated as continuous
            fit(trainingData) 
        // Setting up Random Forest Model
        val rf  = new RandomForestClassifier().
            setFeaturesCol("features").
            setLabelCol(labelIndexer.getOutputCol).
            setNumTrees(nTrees).
            setFeatureSubsetStrategy("auto"). //letting model figure out optimal strategy
            setImpurity("gini").
            setMaxDepth(4).
            setMaxBins(12).
            setSeed(seed)
        /*val gbt = new GBTClassifier().
        setLabelCol(labelIndexer.getOutputCol).
        setFeaturesCol(featuresIndexer.getOutputCol).
        setMaxIter(10)*/
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
        /*// Predict on Training data.
        val trainPredictions = model.transform(trainingData)       
        // Select (prediction, true label) and compute training error
        val precisionEvaluator = new MulticlassClassificationEvaluator().setLabelCol("indexedLabel").setPredictionCol("prediction").setMetricName("precision")
        val trainPrecision = precisionEvaluator.evaluate(trainPredictions)
        println("Training Error = " + (1.0 - trainPrecision))*/
        // Output Feature Importances to File(s)
        val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
        sc.parallelize(rfModel.
                        featureImportances.toArray, 1). //Parellizing to one reducer to avoid multiple output files
                        saveAsTextFile(featureImportancesPath)
        // Creating test dataframe
        val testDf = sc.textFile(testFile).
                        map(_.split("\t")).
                        map(f => FlightTest(f(0), f(1), f(2).toDouble, 
                            f(3).toDouble, f(4).toDouble, f(5).toDouble, 
                            f(6).toDouble, f(7).toDouble, f(8).toDouble, 
                            f(9).toDouble, f(10).toDouble, f(11).toDouble, 
                            f(12).toDouble)).
                        toDF()
        //Fetch the Validation File which has the actual values
        val validateDf = sc.textFile(validateFile).
                            map(_.split(",")).
                            map(v => FlightVal(v(0), v(1))).
                            toDF()
        // Change logical Label to numeric
        val logicalToDouble = udf[Double, String]( _ match { case "FALSE" => 0.0 case "TRUE" => 1.0} )
        val valDf = validateDf.withColumn("label", 
                                logicalToDouble(validateDf("logicalLabel"))).
                    select("id", "label")        
        val joinedDf = testDf.join(valDf, "id") // Join both test and validation dataframes on "id" column
        // Select required feature columns from testing dataframe
        val testingData = assembler.transform(joinedDf)
        val featureIndexedTestingDf = featureIndexer.transform(testingData)
        // Predict on Testing data
        val testPredictions = model.transform(featureIndexedTestingDf)
        testPredictions.select("id", "label", "predictedLabel").
                        rdd.        // convert from Dataframe to RDD for saving as Text File
                        saveAsTextFile(testPredictionsPath) // Save Predictons        
        /*        // Evaluate Test Predictions
        val precisionEvaluator = new MulticlassClassificationEvaluator().
                                    setLabelCol("indexedLabel").
                                    setPredictionCol("prediction").
                                    setMetricName("precision")
        val testPrecision = precisionEvaluator.evaluate(testPredictions) // precision
        
        val recallEvaluator = new MulticlassClassificationEvaluator().
                                setLabelCol("indexedLabel").
                                setPredictionCol("prediction").
                                setMetricName("recall")
        val testRecall = recallEvaluator.evaluate(testPredictions) // recall
        
        val f1Evaluator = new MulticlassClassificationEvaluator().
                            setLabelCol("indexedLabel").
                            setPredictionCol("prediction").
                            setMetricName("f1")
        val testF1 = f1Evaluator.evaluate(testPredictions) // F-1
        // Output Metrics as a List
        sc.parallelize(List(testPrecision, testRecall, testF1), 1).
            saveAsTextFile(metricsPath)*/
        sc.stop()   //stop spark context        
    }
}
