#file to implement k-means & PCA
from pyspark.ml.feature import VectorAssembler, PCA
from pyspark.ml.clustering import KMeans
from pyspark.sql import DataFrame
"""
NOTE:
We should probably have some sort of analysis about the cluster evaluation 
inside of the jupyter notebook. 

Can't think of a good reason to put this in class so leaving it as functions
to keep everything lightweight. 

Interaction/orchestration between all of these functions and processData.py will 
be handled in main.py
"""
def getFeatureVector(data:DataFrame):
    """
    In = data from processData.py, Out = feature vector
    """
    featureVector = VectorAssembler(inputCols=data.columns, outputCol="features")
    cleanedData = featureVector.transform(data)
    return cleanedData

def runK_Means(cleanedData:DataFrame,k:int):
    """
    In = feature vector, Out = k-means model
    PARAMS = k ~ number of clusters, cleanedData ~ feature vector
    Run k-means on dataset. 
    """
    k_means = KMeans(k=k).setMaxIter(10).setSeed(1)
    k_means_model = k_means.fit(cleanedData)
    output = k_means_model.transform(cleanedData)
    return output

def dimensionalityReduction(cleanedData:DataFrame,k:int):
    """
    In = feature vector, Out = Principal components based on k
    PARAMS = k ~ reduced number of features, cleanedData ~ feature vector
    this can be used to reduce the number of features for k-means,
    but note that k-means needs to be ran again after this for comparison.
    """
    pca_reduction = PCA(k=k,inputCol="features").setOutPutCol("reduced_features")
    pca_model = pca_reduction.fit(cleanedData)
    output = pca_model.transform(cleanedData)
    return output