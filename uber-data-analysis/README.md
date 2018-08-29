# Uber Data Analysis
# Where do we get these DataSets? <br>
http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml <br>
http://www.nyc.gov/html/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf <br>
http://data.beta.nyc/dataset/uber-trip-data-foiled-apr-sep-2014 <br>
https://developer.uber.com/docs/businesses/data-automation/data-download <br>


#  Detailed Explaination of Uber Data Analysis

1) Why this phase is needed? <br>
   This phase is needed to study about the historical data, and observe the pattern recognition of the uber system which is needed. Based on this we can arrive a conclusion for better decision making and predictions.<br>
 
 2) What is analytics using the model? <br>
   This is the second phase of the project,uses the model in production on live events, it still needed to do an analyis of historical data. <br>
![alt text](https://github.com/Pkrish15/uber-datanalysis/blob/master/1.jpg)<br>

3) What Algorithm choosed suitable for Data Analytics? <br>

Clustering uses unsupervised algorithms, which do not have the outputs (labeled data) in advance. K-means is one of the most commonly used clustering algorithms that clusters the data points into a predefined number of clusters (k). Clustering using the K-means algorithm begins by initializing all the coordinates to k number of centroids. With every pass of the algorithm, each point is assigned to its nearest centroid based on some distance metric, which is usually Euclidean distance. The centroids are then updated to be the “centers” of all the points assigned to it in that pass. This repeats until there is a minimum change in the centers. <br>

4) What is in the data set? <br>

The Data Set Schema
Date/Time: The date and time of the Uber pickup
Lat: The latitude of the Uber pickup
Lon: The longitude of the Uber pickup
Base: The TLC base company affiliated with the Uber pickup
​​The Data Records are in CSV format. An example line is shown below:

2014-08-01 00:00:00,40.729,-73.9422,B02598 <br>

5) How do we do it? <br>

Load the data into a Spark Data Frame <br>

![alt text](https://github.com/Pkrish15/uber-datanalysis/blob/master/2.png)<br> <br>

Define Features Array <br>
In order for the features to be used by a machine learning algorithm, the features are transformed and put into Feature Vectors, which are vectors of numbers representing the value for each feature. Below, a VectorAssembler is used to transform and return a new DataFrame with all of the feature columns in a vector column. <br>

![alt text](https://github.com/Pkrish15/uber-datanalysis/blob/master/3.png)<br> <br>

Create a KMeans Object, set the parameters to define the number of clusters and the maximum number of iterations to determine the clusters and then we fit our model to the input data.

![alt text](https://github.com/Pkrish15/uber-datanalysis/blob/master/4.png)<br> <br>

Output, Cluster Centers are displayed on the Google Map <br>
![alt text](https://github.com/Pkrish15/uber-datanalysis/blob/master/5.png)<br> <br>

Further Analysis of cluster <br>
![alt text](https://github.com/Pkrish15/uber-datanalysis/blob/master/6.png)<br> <br>

6) What are the Analysis Questions? <br>

Which hour of the day and which cluster had highest number of pickups?<br>

How many pickups occured in each cluster? <br>

7) How can we deploy in OpenShift? <br>

# Implementation on Openshift <br>
   a) oc new-project pk-zp <br> <br>
   b) oc create -f https://raw.githubusercontent.com/Pkrish15/uber-datanalysis/master/zeppelin-openshift.yaml <br> <br>
   c) oc new-app --template=$namespace/apache-zeppelin-openshift \
    --param=APPLICATION_NAME=apache-zeppelin \
    --param=GIT_URI=https://github.com/rimolive/zeppelin-notebooks.git \
    --param=ZEPPELIN_INTERPRETERS=md       <br><br>
 
 2) Create a PVC in the pod claiming 2GiB <br><br>
 3) Copy the Local Data to the Pod Directory using Rsync Command <br><br>
     oc rsync /home/prakrish/workspace/uberdata-analysis/src/main/resources/data/  apache-zeppelin-2-f89tz:/data <br>
     oc rsync src directory pod directory:/data This is a format <br> <br>
 
 4) Open the Zeppellin notebook <br> <br>
    oc get route <br>
    http://apache-zeppelin-pk-zp.apps.dev39.openshift.opentlc.com/#/
    
 5) Import the JSON Notebook in the Zepplin Notebook <br> <br>
    Currently it is in our GitHub URL <br>
    https://raw.githubusercontent.com/Pkrish15/uber-datanalysis/master/Uber%20Data%20Analysis.json <br> <br>
    
 6) Run the Zepplin Notebook at every stages, Please read the zepplin tutorial if required. <br> <br>
 
 # References 
 
 https://www.youtube.com/watch?v=52zTo7bznXw <br>
 
 https://www.youtube.com/watch?v=nncxYGD6m7E <br>
 



















   
   
 
 
    
 
    
 
    
 
 

   


   
   
