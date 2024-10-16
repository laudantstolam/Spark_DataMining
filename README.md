### Getinto Spark

#### ENV

Master--Microsoft Windows (10.0.22631.4317)
Worker--Virtural Box-Ubuntu 22.04

#### Start my spark master in Win

`spark-class org.apache.spark.deploy.master.Master`

http://ash:8080/

![](/HW0/Master.png)


#### Start my spark worker in Ubuntu

`spark-class org.apache.spark.deploy.worker.Worker spark://192.168.1.107:7077`

http://localhost:8081/

![](/HW0/Worker.png)

#### Language
python

---

>Brief write up of the HW0

1. get datasets from https://archive.ics.uci.edu/ml/datasets/individual+household+electric+power+consumption as data.txt

1. pre-processing by droping nulls

1. find the key values with Dataframe

1. calculate the max-min normalization(in result_3.csv)

![](/HW0/result_1.png)
![](/HW0/result_2.png)
