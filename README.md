# Forest Fires Cluster Analysis & Dimensionality Reduction

## Overview

That goal of this repository is to classify forest fires with K-means for clustering and PCA for dimensionality reduction.  
The repository is based on the Kaggle dataset: https://www.kaggle.com/datasets/rtatman/188-million-us-wildfires  

The repository is using PySpark for data transformations and modelling. The two primary modules are: 
1. processData.py for data transformations and cleaning
2. modelData.py for running the K-means and PCA models

## Usage

The repository is containerized and should be deployed with docker. 

Build = docker build -t imagename:imagetag .  

Run = docker run -it -v /local/path/to/data/folder:/app/data imageName

Run application = python3 src/main.py -k <# of clusters> -p <# of dimensions>

## Output

Once the code is ran as described above it will write a csv that has the cluster predictions to directory "/local/path/to/data/folder"  
The output can then be used for visualization purposes such as Tableau. 
