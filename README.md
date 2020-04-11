# Spark

<p align="center"> 
<img src="https://github.com/BardisRenos/Spark/blob/master/spark.jpg" width="450" height="250" style=centerme>
</p>


In this repository will explain the Apache Spark an open source distributed general purpose cluster computing framework. Spark provides an interface for programming entire clusters with implicit data parallelism and fault tolerance. Apache Spark requires a cluster manager and a distributed storage system. For cluster management, Spark supports standalone.


<p align="center"> 
<img src="https://github.com/BardisRenos/Spark/blob/master/cluster-overview.png" width="450" height="350" style=centerme>
</p>

### 1 Create a dataframe from the source with Spark

Spark can retrieve raw data from the source in different types.

```python

  # JSON File
  dataframe = sc.read.json("the/path/of/the/file.json")
  
  # TXT File 
  dataframe_txt = sc.read.text("the/path/of/the/file.txt")
  
  # CSV File
  dataframe_csv = sc.read.csv("the/path/of/the/file.csv")
```

*The raw data from the file is transformed into a spark dataframe format.* 

### 1.1 Shows the structure of the dataframe 

Print only the columns of the dataframe 

```python
  dataframe = sc.read.json('file.json')
  print(dataframe.columns)
```

``
['_id', 'amazon_product_url', 'author', 'bestsellers_date', 'description', 'price', 'published_date', 'publisher', 'rank', 'rank_last_week', 'title', 'weeks_on_list']
``

The below command shows the structure of the dataframe. Describe each column of the type. 

```python
  print(dataframe.printSchema())
```

``
  |root|
  |------|
   |-- _id: struct (nullable = true)
   |    |-- $oid: string (nullable = true)
   |-- amazon_product_url: string (nullable = true)
   |-- author: string (nullable = true)
   |-- bestsellers_date: struct (nullable = true)
   |    |-- $date: struct (nullable = true)
   |    |    |-- $numberLong: string (nullable = true)
   |-- description: string (nullable = true)
   |-- price: struct (nullable = true)
   |    |-- $numberDouble: string (nullable = true)
   |    |-- $numberInt: string (nullable = true)
   |-- published_date: struct (nullable = true)
   |    |-- $date: struct (nullable = true)
   |    |    |-- $numberLong: string (nullable = true)
   |-- publisher: string (nullable = true)
   |-- rank: struct (nullable = true)
   |    |-- $numberInt: string (nullable = true)
   |-- rank_last_week: struct (nullable = true)
   |    |-- $numberInt: string (nullable = true)
   |-- title: string (nullable = true)
   |-- weeks_on_list: struct (nullable = true)
   |    |-- $numberInt: string (nullable = true)
``

### 1.2 How to show the dataframe 

To show the dataframe with all the columns (20 first rows) 

```python
  print(dataframe.show())
```


To show a specific number of rows. The variable **n** indicates the number of rows.
```python
  print(dataframe.show(n))
```


### 1.3 The *select* operation

To select a specific column in spark dataframe. You have to use select command (like in SQL)

```python
  print(dataframe.select("author").show())
 ```

``
|author|
|------|
|       Dean R Koontz|
|     Stephenie Meyer|
|        Emily Giffin|
|   Patricia Cornwell|
|     Chuck Palahniuk|
|James Patterson a...|
|       John Sandford|
|       Jimmy Buffett|
|    Elizabeth George|
|      David Baldacci|
|        Troy Denning|
|          James Frey|
|         Garth Stein|
|     Debbie Macomber|
|         Jeff Shaara|
|    Phillip Margolin|
|       Jhumpa Lahiri|
|      Joseph O'Neill|
|        John Grisham|
|       James Rollins|
+--------------------+
only showing top 20 rows
``


If you want to select more than one column then You need to write:

```python
  print(dataframe.select("author", "price").show(5))
  
  # Alternative command is 
  print(dataframe.select(dataframe["author"], dataframe["price"]).show(5))
```

``
| author | price |
|--------|-------|
|    Dean R Koontz|  [, 27]|
|  Stephenie Meyer|[25.99,]|
|     Emily Giffin|[24.95,]|
|Patricia Cornwell|[22.95,]|
|  Chuck Palahniuk|[24.95,]|
+-----------------+--------+
only showing top 5 rows
``

### 1.4 The *when* operation



