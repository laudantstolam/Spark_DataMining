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

### Brief write up of the HW0

1. get datasets from https://archive.ics.uci.edu/ml/datasets/individual+household+electric+power+consumption as data.txt

1. pre-processing by droping nulls

1. find the key values with Dataframe

1. calculate the max-min normalization(in result_3.csv)

![](/HW0/result_1.png)
![](/HW0/result_2.png)

### Brief writeup of the HW1

data ref: https://www.kaggle.com/datasets/patrickfleith/space-news-dataset/data

>pre-processing

- formatting date values
- filter text by lowercases and removing punctuations and spaces with nltk library

> data analyze result
![](result/analyze.png)

==since there are null values in "date" index, I'd seperated the data set, df_filter, which drops the null dates will be the datas regardings the analysis dealing with date==

>(1) (2)calculate freuqency

- Calculate total frequency with `df_total` 
- Use `df_filtered`  for date-associated calculations
![](/HW1/result/01_title.png)
![](/HW1/result/0101_title.png)
![](/HW1/result/01_content.png)
![](/HW1/result/0101_content.png)

> (3)calculate article published frequency
- use `df_filtered` 
![](/HW1/result/02.png)
![](/HW1/result/0202.png)

>(4) Records containing 'space' in both 'title' and 'postexcerpt
-  use `df_total` 
- use substring to shorten the data and saved into csv
![](/HW1/result/03.png)