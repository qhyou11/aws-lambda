# GC Reset Detect
A simple lambda to find out the gc count reset for all the DataNodes in core instance groups of all the AWS EMR clusters.
The metric is stored in cloudwatch with namespace of "CustMetrics" and the metric name is "GcCount".
The script will pull latest 10 minitues metrics from cloudwatch, sort them and compare the first and the last record, if latest value was smaller than preview values, then alert it. 

