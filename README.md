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


The below command shows the structure of the dataframe. Describe each column of the type. 

```python
  print(dataframe.printSchema())
```

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


### 1.2 How to show the dataframe 

To show the dataframe with all the columns (20 first rows) 

```python
  print(dataframe.show())
```

To show a specific number of rows. The variable **n** indicates the number of rows.
```python
  print(dataframe.show(n))
```

### 1.3 The shape of the dataframe

In order to print the number of rows and columns

```python
  print(dataframe.count(), len(dataframe.columns))
```
The first number is the number of rows and the second number is the number of columns:
  10195 12


### 1.4 Show the basic statistics for a column .

```python
  print(dataframe.describe(['author']).show())
```

### 1.5 Show the data type of each column

```python
  print(dataframe.dtypes)
```

| Name |
|------|
[('_id', 'struct<$oid:string>'), ('amazon_product_url', 'string'), ('author', 'string'), ('bestsellers_date', 'struct<$date:struct<$numberLong:string>>'), ('description', 'string'), ('price', 'struct<$numberDouble:string,$numberInt:string>'), ('published_date', 'struct<$date:struct<$numberLong:string>>'), ('publisher', 'string'), ('rank', 'struct<$numberInt:string>'), ('rank_last_week', 'struct<$numberInt:string>'), ('title', 'string'), ('weeks_on_list', 'struct<$numberInt:string>')]


### 1.6 The *select* operation

To select a specific column in spark dataframe. You have to use select command (like in SQL)

```python
  print(dataframe.select("author").show())
 ```


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



If you want to select more than one column, then it needs to write:

```python
  print(dataframe.select("author", "price").show(5))
  
  # Alternative command is 
  print(dataframe.select(dataframe["author"], dataframe["price"]).show(5))
```


| author | price |
|--------|-------|
|    Dean R Koontz|  [, 27]|
|  Stephenie Meyer|[25.99,]|
|     Emily Giffin|[24.95,]|
|Patricia Cornwell|[22.95,]|
|  Chuck Palahniuk|[24.95,]|
only showing top 5 rows


### 1.7 The *filter* operation

The filter command, filters rows using the given condition. Both shows the same results. 

```python
  print(dataframe.filter(dataframe.author == "Dean R Koontz").show())
  
  # The where is an alias command (alternative command)
  print(dataframe.where(dataframe.author == "Dean R Koontz").show())
```


### 1.8 The "when" operation
In the first example, the “title” column is selected and a condition is added with a “when” condition.

```python
  print(dataframe.select(dataframe.title, dataframe.author, when(dataframe.author == 'Debbie Macomber', 1).otherwise(0)).show())
```

| title  | author| CASE WHEN (author = Debbie Macomber) THEN 1 ELSE 0 END |
|--------|-------|--------|
|           ODD HOURS|       Dean R Koontz|                                                     0|
|            THE HOST|     Stephenie Meyer|                                                     0|
|LOVE THE ONE YOU'...|        Emily Giffin|                                                     0|
|           THE FRONT|   Patricia Cornwell|                                                     0|
|               SNUFF|     Chuck Palahniuk|                                                     0|
|SUNDAYS AT TIFFANY�S|James Patterson a...|                                                     0|
|        PHANTOM PREY|       John Sandford|                                                     0|
|          SWINE NOT?|       Jimmy Buffett|                                                     0|
|     CARELESS IN RED|    Elizabeth George|                                                     0|
|     THE WHOLE TRUTH|      David Baldacci|                                                     0|
|          INVINCIBLE|        Troy Denning|                                                     0|
|BRIGHT SHINY MORNING|          James Frey|                                                     0|
|THE ART OF RACING...|         Garth Stein|                                                     0|
|       TWENTY WISHES|     Debbie Macomber|                                                     1|
|      THE STEEL WAVE|         Jeff Shaara|                                                     0|
| EXECUTIVE PRIVILEGE|    Phillip Margolin|                                                     0|
|  UNACCUSTOMED EARTH|       Jhumpa Lahiri|                                                     0|
|          NETHERLAND|      Joseph O'Neill|                                                     0|
|          THE APPEAL|        John Grisham|                                                     0|
|INDIANA JONES AND...|       James Rollins|                                                     0|
only showing top 20 rows


The table shows an overall result of the findings. It is easy to count only the results where the value is 1.

```python
 print(dataframe.where(dataframe.author == 'Debbie Macomber').where(dataframe.title == 'TWENTY WISHES').count())
```

``
  The result is:  1
``

### 1.9 Multiple operation
 
In one Spark query can be used multiple operation. Like multiple where.

```python
print(dataframe.select(dataframe.title, dataframe.author, dataframe.publisher).filter(dataframe.author == 'Debbie Macomber').filter(dataframe.publisher == 'Mira').filter(dataframe.title != 'TWENTY WISHES').show())

```

``
|title| author | publisher |
|-----| -------|-----------|
|A CEDAR COVE CHRI...|Debbie Macomber|     Mira|
|A CEDAR COVE CHRI...|Debbie Macomber|     Mira|
|A CEDAR COVE CHRI...|Debbie Macomber|     Mira|
|A CEDAR COVE CHRI...|Debbie Macomber|     Mira|
|A CEDAR COVE CHRI...|Debbie Macomber|     Mira|
|A CEDAR COVE CHRI...|Debbie Macomber|     Mira|
|SUMMER ON BLOSSOM...|Debbie Macomber|     Mira|
|SUMMER ON BLOSSOM...|Debbie Macomber|     Mira|
|SUMMER ON BLOSSOM...|Debbie Macomber|     Mira|
|SUMMER ON BLOSSOM...|Debbie Macomber|     Mira|
|THE PERFECT CHRIS...|Debbie Macomber|     Mira|
|THE PERFECT CHRIS...|Debbie Macomber|     Mira|
|       HANNAH'S LIST|Debbie Macomber|     Mira|
|       HANNAH'S LIST|Debbie Macomber|     Mira|
|       HANNAH'S LIST|Debbie Macomber|     Mira|
|       HANNAH'S LIST|Debbie Macomber|     Mira|
|CALL ME MRS. MIRACLE|Debbie Macomber|     Mira|
|CALL ME MRS. MIRACLE|Debbie Macomber|     Mira|
|  A TURN IN THE ROAD|Debbie Macomber|     Mira|
|  A TURN IN THE ROAD|Debbie Macomber|     Mira|
``

Count the result of the above array.

```python
  print(dataframe.filter(dataframe.author == 'Debbie Macomber').filter(dataframe.publisher == 'Mira').filter(dataframe.title != 'TWENTY WISHES').count())
```
``
 The result is: 30
``


### 2.0 SQL queries with Spark

Another ability is taht, someone can write also SQL queries into spark. Create a register of dataframe as a global temporary view. After 
the global temporary view is tied to a system preserved database `global_temp`.

```python
  dataframe.createGlobalTempView("people")

# The current sql query is the replication of the spark query of the 1.9 
  print(sc.sql("select title from global_temp.people where author = 'Debbie Macomber' and publisher = 'Mira' and title <> 'TWENTY     WISHES'").show())
```



