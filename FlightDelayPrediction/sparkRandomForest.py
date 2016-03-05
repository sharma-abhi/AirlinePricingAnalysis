from pyspark import SparkContext
from pyspark.mllib.classification import LogisticRegressionWithSGD
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import RandomForest
from numpy import array

# def parsePoint(line):
#     values = [float(x) for x in line.split(' ')]
#     #return LabeledPoint(values[-1], values[0:-1])
#     return LabeledPoint(values[0], values[1:])

def parsePoint(line):
    arr = line.split('\t')
    values = [int(x) for x in arr[2:]]
    #return LabeledPoint(values[-1], values[0:-1])
    return LabeledPoint(values[-1], arr[0] + values[0:-1])

sc = SparkContext(appName="RandomForest")

data = sc.textFile("/home/abhijeet/Sharma_Khan_A6/output/a6history/AA-r-00000")
parsedData = data.map(parsePoint)

# labelsAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
# trainErr = labelsAndPreds.filter(lambda (v, p):v != p).count()/float(parsedData.count())
# print("Training Error = " + str(trainErr))



model = RandomForest.trainClassifier(parsedData, numClasses=2, categoricalFeaturesInfo={},
                                     numTrees=3, featureSubsetStrategy="auto",
                                     impurity='gini', maxDepth=4, maxBins=32)

# Evaluate model on test instances and compute test error
predictions = model.predict(parsedData.map(lambda x: x.features))
#predictions = model.predict(testData.map(lambda x: x.features))
#labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
labelsAndPredictions = parsedData.map(lambda lp: lp.label).zip(predictions)
#testErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(testData.count())
trainErr = labelsAndPredictions.filter(lambda (v, p): v != p).count() / float(parsedData.count())
#print('Test Error = ' + str(testErr))
print('Train Error = ' + str(trainErr))
print('Learned classification forest model:')
print(model.toDebugString())



model.save(sc, "myModelPath")
#sameModel = LogisticRegressionModel.load(sc, "myModelPath")

sc.stop()
