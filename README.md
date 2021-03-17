# EQ-Project
 Apache Spark project on my local machine  and problem datasets
Problems
1. Cleanup
A sample dataset of request logs is given in data/DataSample.csv. We consider records that have identical geoinfo and timest as suspicious. Please clean up the sample dataset by filtering out those suspicious request records.
2. Label
Assign each request (from data/DataSample.csv) to the closest (i.e. minimum distance) POI (from data/POIList.csv).

Note: a POI is a geographical Point of Interest.

3. Analysis
For each POI, calculate the average and standard deviation of the distance between the POI to each of its assigned requests.
At each POI, draw a circle (with the center at the POI) that includes all of its assigned requests. Calculate the radius and density (requests/area) for each POI.
4. Data Science/Engineering Tracks
Please complete either 4a or 4b. Extra points will be awarded for completing both tasks.

4a. Model
To visualize the popularity of each POI, they need to be mapped to a scale that ranges from -10 to 10. Please provide a mathematical model to implement this, taking into consideration of extreme cases and outliers. Aim to be more sensitive around the average and provide as much visual differentiability as possible.
Bonus: Try to come up with some reasonable hypotheses regarding POIs, state all assumptions, testing steps and conclusions. Include this as a text file (with a name bonus) in your final submission.
