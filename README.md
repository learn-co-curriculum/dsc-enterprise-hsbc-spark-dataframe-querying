
# Querying `DataFrame` 

## Retrieving our Books


```scala
import org.apache.spark.sql.types._
val bookSchema = new StructType(Array(
   new StructField("bookID", IntegerType, false),
   new StructField("title", StringType, false),
   new StructField("authors", StringType, false),
   new StructField("average_rating", FloatType, false),
   new StructField("isbn", StringType, false),
   new StructField("isbn13", StringType, false),
   new StructField("language_code", StringType, false),
   new StructField("num_pages", IntegerType, false),
   new StructField("ratings_count", IntegerType, false),
   new StructField("text_reviews_count", IntegerType, false)))

val booksDF = spark.read.format("csv")
                         .schema(bookSchema)
                         .option("header", "true")
                         .load("../data/books.csv")
booksDF.printSchema()
```

    root
     |-- bookID: integer (nullable = true)
     |-- title: string (nullable = true)
     |-- authors: string (nullable = true)
     |-- average_rating: float (nullable = true)
     |-- isbn: string (nullable = true)
     |-- isbn13: string (nullable = true)
     |-- language_code: string (nullable = true)
     |-- num_pages: integer (nullable = true)
     |-- ratings_count: integer (nullable = true)
     |-- text_reviews_count: integer (nullable = true)
    
    




    import org.apache.spark.sql.types._
    bookSchema: org.apache.spark.sql.types.StructType = StructType(StructField(bookID,IntegerType,false), StructField(title,StringType,false), StructField(authors,StringType,false), StructField(average_rating,FloatType,false), StructField(isbn,StringType,false), StructField(isbn13,StringType,false), StructField(language_code,StringType,false), StructField(num_pages,IntegerType,false), StructField(ratings_count,IntegerType,false), StructField(text_reviews_count,IntegerType,false))
    booksDF: org.apache.spark.sql.DataFrame = [bookID: int, title: string ... 8 more fields]
    



## Filtering Rows

### `where`

* Can be applied to filter rows from the `DataFrame`/`DataSet`
* Can take as an argument
  * A `Column` with some criteria
  * A `String` that represents a query
* Returns a new `DataFrame` with the query results

Here we will procure a `Column` either using `df("..")`, `$".."`, `'..`, `col(..)`, or `column(..)` where `..` is the name of the column. Also please open the [column API reference](https://spark.apache.org/docs/2.3.0/api/scala/index.html#org.apache.spark.sql.Column) to see what kind of calls can be made.


```scala
val stephenKing = booksDF.where($"authors".contains("Stephen King"))
stephenKing.show(10)
```

    +------+--------------------+--------------------+--------------+----------+-------------+-------------+---------+-------------+------------------+
    |bookID|               title|             authors|average_rating|      isbn|       isbn13|language_code|num_pages|ratings_count|text_reviews_count|
    +------+--------------------+--------------------+--------------+----------+-------------+-------------+---------+-------------+------------------+
    |  4978|Wolves of the Cal...|Stephen King-Bern...|          4.19|141651693X|9781416516934|          eng|      931|       120906|              2640|
    |  5094|The Drawing of th...|        Stephen King|          4.23|0451210859|9780451210852|          eng|      463|       163647|              4846|
    |  5095|The Waste Lands (...|        Stephen King|          4.24|034082977X|9780340829776|          eng|      584|         1073|                82|
    |  5096|Wizard and Glass ...|Stephen King-Dave...|          4.25|0340829788|9780340829783|          eng|      845|       122796|              3396|
    |  5098|The Gunslinger (T...|        Stephen King|          3.96|0340829753|9780340829752|          eng|      238|         1598|               189|
    |  5373|The Waste Lands (...|Stephen King-Ned ...|          4.24|0747411875|9780747411871|          eng|      509|           73|                 6|
    |  5399|           The Stand|        Stephen King|          4.34|1568495714|9781568495712|          eng|     1344|          412|                33|
    |  5412|The Stand: Das le...|Stephen King-Joac...|          4.34|3404134117|9783404134113|          ger|     1227|          229|                14|
    |  5413|        'Salem's Lot|Stephen King-Jerr...|          4.25|0385516487|9780385516488|          eng|      594|        81170|               504|
    |  5414|        'Salem's Lot|Stephen King-Ron ...|          4.01|0743536967|9780743536967|          eng|       17|          227|                55|
    +------+--------------------+--------------------+--------------+----------+-------------+-------------+---------+-------------+------------------+
    only showing top 10 rows
    
    




    stephenKing: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [bookID: int, title: string ... 8 more fields]
    



### `where` with `like`

Here we will run the same query as above but using `like`, and using the `col` method to procure a column


```scala
val stephenKing = booksDF.where(col("authors").like("Stephen King"))
stephenKing.show(10)
```

    +------+--------------------+------------+--------------+----------+-------------+-------------+---------+-------------+------------------+
    |bookID|               title|     authors|average_rating|      isbn|       isbn13|language_code|num_pages|ratings_count|text_reviews_count|
    +------+--------------------+------------+--------------+----------+-------------+-------------+---------+-------------+------------------+
    |  5094|The Drawing of th...|Stephen King|          4.23|0451210859|9780451210852|          eng|      463|       163647|              4846|
    |  5095|The Waste Lands (...|Stephen King|          4.24|034082977X|9780340829776|          eng|      584|         1073|                82|
    |  5098|The Gunslinger (T...|Stephen King|          3.96|0340829753|9780340829752|          eng|      238|         1598|               189|
    |  5399|           The Stand|Stephen King|          4.34|1568495714|9781568495712|          eng|     1344|          412|                33|
    |  5415|        'Salem's Lot|Stephen King|          4.01|0965772411|9780965772419|          eng|      405|          959|               121|
    |  5416|The Shining / Sal...|Stephen King|          4.67|0905712609|9780905712604|          eng|      991|         1838|                27|
    |  5417|Carrie / 'Salem's...|Stephen King|          4.53|0517219026|9780517219027|          eng|     1096|        12320|                58|
    |  5418|        'Salem's Lot|Stephen King|          4.01|0451150651|9780451150653|          eng|      446|          399|                45|
    |  5419|        'Salem's Lot|Stephen King|          4.01|0451092317|9780451092311|        en-US|      427|          169|                33|
    |  5420|        'Salem's Lot|Stephen King|          4.01|0340770538|9780340770535|          eng|      586|           24|                 6|
    +------+--------------------+------------+--------------+----------+-------------+-------------+---------+-------------+------------------+
    only showing top 10 rows
    
    




    stephenKing: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [bookID: int, title: string ... 8 more fields]
    



### Using `where` with a `String`

Here we will use a `String` with a SQL like format, which will be a nice segue to SparkSQL. For more on the SQL calls that can be made with `where`, [here is a handy reference](https://spark.apache.org/docs/2.3.0/api/sql/index.html)


```scala
val stephenKing = booksDF.where("title like '%Stephen King%'")
stephenKing.show()
```

    +------+--------------------+--------------------+--------------+----------+-------------+-------------+---------+-------------+------------------+
    |bookID|               title|             authors|average_rating|      isbn|       isbn13|language_code|num_pages|ratings_count|text_reviews_count|
    +------+--------------------+--------------------+--------------+----------+-------------+-------------+---------+-------------+------------------+
    | 10586|The Stephen King ...|Stephen King-John...|          3.99|0739317369|9780739317365|          eng|       11|           55|                 9|
    | 10594|Stephen King: Ame...|        George Beahm|          3.79|0836254279|9780836254273|        en-US|      304|           49|                 3|
    | 10597|The Illustrated S...|Brian James Freem...|          4.23|1587671166|9781587671166|          eng|      404|           22|                 5|
    | 10604|The Body Snatcher...|Jack Finney-Steph...|          3.89|1582881804|9781582881805|          eng|      224|          181|                10|
    | 10606|Stephen King from...|        George Beahm|          3.83|0836269144|9780836269147|          eng|      251|           53|                 2|
    | 10612|The Stephen King ...|Stanley Wiater-Ch...|          4.14|1580631606|9781580631600|        en-US|      478|         6241|                37|
    | 11601|The Science of St...|Lois H. Gresh-Rob...|          3.49|0471782475|9780471782476|          eng|      272|           58|                 9|
    | 11606|The Bachman Books...|Stephen King-Rich...|          4.11|0453005071|9780453005074|          eng|      692|         1064|                81|
    | 12676|Carrie (Bibliotec...|Stephen King-Greg...|          3.95|0609810901|9780609810903|          spa|      288|           64|                 3|
    | 32664|The Stephen King ...|Stephen King-John...|          3.99|0553527401|9780553527407|          eng|        9|          659|                22|
    | 32689|Bare Bones: Conve...|Tim Underwood-Chu...|          4.16|0446390577|9780446390576|          eng|      224|         3050|                23|
    +------+--------------------+--------------------+--------------+----------+-------------+-------------+---------+-------------+------------------+
    
    




    stephenKing: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [bookID: int, title: string ... 8 more fields]
    



### `filter` 

* `filter` takes a functional approach. 
* `DataFrame` and `Dataset` are the same
  * A `DataFrame` is a `Dataset[Row]`
* Has the same signature as `where` but also has the ability to select information using a Scala function


```scala
val stephenKing = booksDF.filter(booksDF("authors").contains("Stephen King"))
stephenKing.show(5)
```

    +------+--------------------+--------------------+--------------+----------+-------------+-------------+---------+-------------+------------------+
    |bookID|               title|             authors|average_rating|      isbn|       isbn13|language_code|num_pages|ratings_count|text_reviews_count|
    +------+--------------------+--------------------+--------------+----------+-------------+-------------+---------+-------------+------------------+
    |  4978|Wolves of the Cal...|Stephen King-Bern...|          4.19|141651693X|9781416516934|          eng|      931|       120906|              2640|
    |  5094|The Drawing of th...|        Stephen King|          4.23|0451210859|9780451210852|          eng|      463|       163647|              4846|
    |  5095|The Waste Lands (...|        Stephen King|          4.24|034082977X|9780340829776|          eng|      584|         1073|                82|
    |  5096|Wizard and Glass ...|Stephen King-Dave...|          4.25|0340829788|9780340829783|          eng|      845|       122796|              3396|
    |  5098|The Gunslinger (T...|        Stephen King|          3.96|0340829753|9780340829752|          eng|      238|         1598|               189|
    +------+--------------------+--------------------+--------------+----------+-------------+-------------+---------+-------------+------------------+
    only showing top 5 rows
    
    




    stephenKing: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [bookID: int, title: string ... 8 more fields]
    




```scala
val jkRowling = booksDF.filter("authors like '%J.K. Rowling%'")
jkRowling.show(5)
```

    +------+--------------------+--------------------+--------------+----------+-------------+-------------+---------+-------------+------------------+
    |bookID|               title|             authors|average_rating|      isbn|       isbn13|language_code|num_pages|ratings_count|text_reviews_count|
    +------+--------------------+--------------------+--------------+----------+-------------+-------------+---------+-------------+------------------+
    |     1|Harry Potter and ...|J.K. Rowling-Mary...|          4.56|0439785960|9780439785969|          eng|      652|      1944099|             26249|
    |     2|Harry Potter and ...|J.K. Rowling-Mary...|          4.49|0439358078|9780439358071|          eng|      870|      1996446|             27613|
    |     3|Harry Potter and ...|J.K. Rowling-Mary...|          4.47|0439554934|9780439554930|          eng|      320|      5629932|             70390|
    |     4|Harry Potter and ...|        J.K. Rowling|          4.41|0439554896|9780439554893|          eng|      352|         6267|               272|
    |     5|Harry Potter and ...|J.K. Rowling-Mary...|          4.55|043965548X|9780439655484|          eng|      435|      2149872|             33964|
    +------+--------------------+--------------------+--------------+----------+-------------+-------------+---------+-------------+------------------+
    only showing top 5 rows
    
    




    jkRowling: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [bookID: int, title: string ... 8 more fields]
    



## `filter` using functional programming

* We can use functional programming to filter out a `DataFrame`/`Dataset`
* That includes a `filter` that will query information based on `Row`
* Seek out the [Row Spark API](https://spark.apache.org/docs/2.3.0/api/scala/index.html#org.apache.spark.sql.Row) on how to dig into the row and get the information you desire
* Here we will use something different called `getAs[T]` where `[T]` is the generic type


```scala
val badBooks = booksDF.filter(row => row.getAs[Float]("average_rating") < 3)
badBooks.show(5)
```

    +------+--------------------+--------------------+--------------+----------+-------------+-------------+---------+-------------+------------------+
    |bookID|               title|             authors|average_rating|      isbn|       isbn13|language_code|num_pages|ratings_count|text_reviews_count|
    +------+--------------------+--------------------+--------------+----------+-------------+-------------+---------+-------------+------------------+
    |   159|Dinner with Anna ...|    Gloria Goldreich|          2.96|0778322270|9780778322276|          eng|      368|          400|                64|
    |   799|Out to Eat London...|Lonely Planet-Mar...|           0.0|1740592050|9781740592055|          eng|      295|            0|                 0|
    |  1302|Juiced Official S...|          Doug Walsh|           0.0|0744005612|9780744005615|          eng|      112|            0|                 0|
    |  1584|Cliffs Notes on A...|    W. John Campbell|          2.33|0822007762|0049086007763|          eng|       80|            3|                 0|
    |  1658|American Governme...|Karen  O'Connor-L...|          2.95|0321317106|9780321317100|          eng|      664|            0|                 0|
    +------+--------------------+--------------------+--------------+----------+-------------+-------------+---------+-------------+------------------+
    only showing top 5 rows
    
    




    badBooks: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [bookID: int, title: string ... 8 more fields]
    



### Query columns using `select`

* `select` selects which columns and aliases you can apply to your query
* Works very much the way `SELECT` works on a SQL Query
* STOP and remember: `select` for columns, `where` and `filter` for rows
* Again, worth restating that it is worthwhile to [use the Column API](https://spark.apache.org/docs/2.3.0/api/scala/index.html#org.apache.spark.sql.Column)


```scala
val bookTitlesOnly = booksDF.select("title")
bookTitlesOnly.show(5, truncate=false)
```

    +------------------------------------------------------------+
    |title                                                       |
    +------------------------------------------------------------+
    |Harry Potter and the Half-Blood Prince (Harry Potter  #6)   |
    |Harry Potter and the Order of the Phoenix (Harry Potter  #5)|
    |Harry Potter and the Sorcerer's Stone (Harry Potter  #1)    |
    |Harry Potter and the Chamber of Secrets (Harry Potter  #2)  |
    |Harry Potter and the Prisoner of Azkaban (Harry Potter  #3) |
    +------------------------------------------------------------+
    only showing top 5 rows
    
    




    bookTitlesOnly: org.apache.spark.sql.DataFrame = [title: string]
    




```scala
val bookTitlesOnly = booksDF.select(col("title"))
bookTitlesOnly.show(5, truncate=false)
```

    +------------------------------------------------------------+
    |title                                                       |
    +------------------------------------------------------------+
    |Harry Potter and the Half-Blood Prince (Harry Potter  #6)   |
    |Harry Potter and the Order of the Phoenix (Harry Potter  #5)|
    |Harry Potter and the Sorcerer's Stone (Harry Potter  #1)    |
    |Harry Potter and the Chamber of Secrets (Harry Potter  #2)  |
    |Harry Potter and the Prisoner of Azkaban (Harry Potter  #3) |
    +------------------------------------------------------------+
    only showing top 5 rows
    
    




    bookTitlesOnly: org.apache.spark.sql.DataFrame = [title: string]
    




```scala
val bookTitlesOnly = booksDF.select(col("title"), $"authors")
bookTitlesOnly.show(5, truncate=false)
```

    +------------------------------------------------------------+--------------------------+
    |title                                                       |authors                   |
    +------------------------------------------------------------+--------------------------+
    |Harry Potter and the Half-Blood Prince (Harry Potter  #6)   |J.K. Rowling-Mary GrandPré|
    |Harry Potter and the Order of the Phoenix (Harry Potter  #5)|J.K. Rowling-Mary GrandPré|
    |Harry Potter and the Sorcerer's Stone (Harry Potter  #1)    |J.K. Rowling-Mary GrandPré|
    |Harry Potter and the Chamber of Secrets (Harry Potter  #2)  |J.K. Rowling              |
    |Harry Potter and the Prisoner of Azkaban (Harry Potter  #3) |J.K. Rowling-Mary GrandPré|
    +------------------------------------------------------------+--------------------------+
    only showing top 5 rows
    
    




    bookTitlesOnly: org.apache.spark.sql.DataFrame = [title: string, authors: string]
    




```scala
val bookTitlesOnly = booksDF.select(col("title"), $"authors", ($"average_rating" > 4.5).as("highly_rated"))
bookTitlesOnly.show(5, truncate=false)
```

    +------------------------------------------------------------+--------------------------+------------+
    |title                                                       |authors                   |highly_rated|
    +------------------------------------------------------------+--------------------------+------------+
    |Harry Potter and the Half-Blood Prince (Harry Potter  #6)   |J.K. Rowling-Mary GrandPré|true        |
    |Harry Potter and the Order of the Phoenix (Harry Potter  #5)|J.K. Rowling-Mary GrandPré|false       |
    |Harry Potter and the Sorcerer's Stone (Harry Potter  #1)    |J.K. Rowling-Mary GrandPré|false       |
    |Harry Potter and the Chamber of Secrets (Harry Potter  #2)  |J.K. Rowling              |false       |
    |Harry Potter and the Prisoner of Azkaban (Harry Potter  #3) |J.K. Rowling-Mary GrandPré|true        |
    +------------------------------------------------------------+--------------------------+------------+
    only showing top 5 rows
    
    




    bookTitlesOnly: org.apache.spark.sql.DataFrame = [title: string, authors: string ... 1 more field]
    



### Using `expr`

* Uses SQL-Like expressions to select which columns and which kind of manipulations
* Can contain just the column name
* Can also contain some intricate queries


```scala
val bookTitlesOnly = booksDF.select(expr("title"), expr("authors"), expr("(average_rating > 4.5) as highly_rated"))
bookTitlesOnly.show(5, truncate=false)
```

    +------------------------------------------------------------+--------------------------+------------+
    |title                                                       |authors                   |highly_rated|
    +------------------------------------------------------------+--------------------------+------------+
    |Harry Potter and the Half-Blood Prince (Harry Potter  #6)   |J.K. Rowling-Mary GrandPré|true        |
    |Harry Potter and the Order of the Phoenix (Harry Potter  #5)|J.K. Rowling-Mary GrandPré|false       |
    |Harry Potter and the Sorcerer's Stone (Harry Potter  #1)    |J.K. Rowling-Mary GrandPré|false       |
    |Harry Potter and the Chamber of Secrets (Harry Potter  #2)  |J.K. Rowling              |false       |
    |Harry Potter and the Prisoner of Azkaban (Harry Potter  #3) |J.K. Rowling-Mary GrandPré|true        |
    +------------------------------------------------------------+--------------------------+------------+
    only showing top 5 rows
    
    




    bookTitlesOnly: org.apache.spark.sql.DataFrame = [title: string, authors: string ... 1 more field]
    



### `selectExpr`

* Since it is so common to use `select` and `expr` together there is an all-in-one command called `selectExpr`


```scala
val bookTitlesOnly = booksDF.selectExpr("title", "authors", "(average_rating > 4.5) as highly_rated")
bookTitlesOnly.show(5, truncate=false)
```

    +------------------------------------------------------------+--------------------------+------------+
    |title                                                       |authors                   |highly_rated|
    +------------------------------------------------------------+--------------------------+------------+
    |Harry Potter and the Half-Blood Prince (Harry Potter  #6)   |J.K. Rowling-Mary GrandPré|true        |
    |Harry Potter and the Order of the Phoenix (Harry Potter  #5)|J.K. Rowling-Mary GrandPré|false       |
    |Harry Potter and the Sorcerer's Stone (Harry Potter  #1)    |J.K. Rowling-Mary GrandPré|false       |
    |Harry Potter and the Chamber of Secrets (Harry Potter  #2)  |J.K. Rowling              |false       |
    |Harry Potter and the Prisoner of Azkaban (Harry Potter  #3) |J.K. Rowling-Mary GrandPré|true        |
    +------------------------------------------------------------+--------------------------+------------+
    only showing top 5 rows
    
    




    bookTitlesOnly: org.apache.spark.sql.DataFrame = [title: string, authors: string ... 1 more field]
    




```scala
val highlyRatedBooksWithPrimaryAuthors = booksDF.selectExpr("title", "(split(authors, '-')[0])", "(average_rating > 4.5)")
highlyRatedBooksWithPrimaryAuthors.show(5, truncate=false)

```

    +------------------------------------------------------------+--------------------+----------------------+
    |title                                                       |split(authors, -)[0]|(average_rating > 4.5)|
    +------------------------------------------------------------+--------------------+----------------------+
    |Harry Potter and the Half-Blood Prince (Harry Potter  #6)   |J.K. Rowling        |true                  |
    |Harry Potter and the Order of the Phoenix (Harry Potter  #5)|J.K. Rowling        |false                 |
    |Harry Potter and the Sorcerer's Stone (Harry Potter  #1)    |J.K. Rowling        |false                 |
    |Harry Potter and the Chamber of Secrets (Harry Potter  #2)  |J.K. Rowling        |false                 |
    |Harry Potter and the Prisoner of Azkaban (Harry Potter  #3) |J.K. Rowling        |true                  |
    +------------------------------------------------------------+--------------------+----------------------+
    only showing top 5 rows
    
    




    highlyRatedBooksWithPrimaryAuthors: org.apache.spark.sql.DataFrame = [title: string, split(authors, -)[0]: string ... 1 more field]
    



### Applying an alias to our `select` columns


```scala
val highlyRatedBooksWithPrimaryAuthors = booksDF.selectExpr("title", "(split(authors, '-')[0]) as primary_author", "(average_rating > 4.5) as highly_rated")
highlyRatedBooksWithPrimaryAuthors.show(5, truncate=false)

```

    +------------------------------------------------------------+--------------+------------+
    |title                                                       |primary_author|highly_rated|
    +------------------------------------------------------------+--------------+------------+
    |Harry Potter and the Half-Blood Prince (Harry Potter  #6)   |J.K. Rowling  |true        |
    |Harry Potter and the Order of the Phoenix (Harry Potter  #5)|J.K. Rowling  |false       |
    |Harry Potter and the Sorcerer's Stone (Harry Potter  #1)    |J.K. Rowling  |false       |
    |Harry Potter and the Chamber of Secrets (Harry Potter  #2)  |J.K. Rowling  |false       |
    |Harry Potter and the Prisoner of Azkaban (Harry Potter  #3) |J.K. Rowling  |true        |
    +------------------------------------------------------------+--------------+------------+
    only showing top 5 rows
    
    




    highlyRatedBooksWithPrimaryAuthors: org.apache.spark.sql.DataFrame = [title: string, primary_author: string ... 1 more field]
    



## Sorting a `DataFrame`

* We can sort the data with the `sort` method providing a column. 
* The default is _ascending order_

Here we are using every knowledge we've accumulated so far to build a solid result

### Sorting in default ascending order 


```scala
booksDF.select($"title", $"isbn", $"authors", $"num_pages")
       .where(($"num_pages".isNotNull) && ($"num_pages" > 30))
       .sort($"num_pages")
       .show(10, truncate=40)
```

    +----------------------------------------+----------+----------------------------------------+---------+
    |                                   title|      isbn|                                 authors|num_pages|
    +----------------------------------------+----------+----------------------------------------+---------+
    |                                The Nose|0688104649|              Nikolai Gogol-Kevin Hawkes|       31|
    |The Cognitive Style of PowerPoint: Pi...|0961392169|                         Edward R. Tufte|       31|
    |     The Chronicles of Narnia CD Box Set|0694524751|              C.S. Lewis-Kenneth Branagh|       31|
    |               Blueberries for the Queen|0066239427|John  Paterson-Katherine Paterson-Sus...|       32|
    |The Little Mouse  the Red Ripe Strawb...|0859533301|                             Don    Wood|       32|
    |Literature Circle Guide: Bridge to Te...|0439271711|                          Tara MacCarthy|       32|
    |Literature Circle Guide: A Wrinkle in...|043927169X|                          Tara MacCarthy|       32|
    |          Secret of the Peaceful Warrior|0915811235|Dan Millman-Robert D. San Souci-T. Ta...|       32|
    |The Sun Rises: and Other Questions Ab...|0753459647|                          Brenda Walpole|       32|
    |I Blink: And Other Questions About My...|0753456109|                           Brigid Avison|       32|
    +----------------------------------------+----------+----------------------------------------+---------+
    only showing top 10 rows
    
    

### Sorting explicitly in ascending order using `Column`'s `asc` method

* Each `Column` has a `asc` method that can be used with `sort` to sort in ascending order


```scala
booksDF.select($"title", $"isbn", $"authors", $"num_pages")
       .where(($"num_pages".isNotNull) && ($"num_pages" > 30))
       .sort($"num_pages".asc)
       .show(10, truncate=40)
```

    +----------------------------------------+----------+----------------------------------------+---------+
    |                                   title|      isbn|                                 authors|num_pages|
    +----------------------------------------+----------+----------------------------------------+---------+
    |                                The Nose|0688104649|              Nikolai Gogol-Kevin Hawkes|       31|
    |The Cognitive Style of PowerPoint: Pi...|0961392169|                         Edward R. Tufte|       31|
    |     The Chronicles of Narnia CD Box Set|0694524751|              C.S. Lewis-Kenneth Branagh|       31|
    |               Blueberries for the Queen|0066239427|John  Paterson-Katherine Paterson-Sus...|       32|
    |The Little Mouse  the Red Ripe Strawb...|0859533301|                             Don    Wood|       32|
    |Literature Circle Guide: Bridge to Te...|0439271711|                          Tara MacCarthy|       32|
    |Literature Circle Guide: A Wrinkle in...|043927169X|                          Tara MacCarthy|       32|
    |          Secret of the Peaceful Warrior|0915811235|Dan Millman-Robert D. San Souci-T. Ta...|       32|
    |The Sun Rises: and Other Questions Ab...|0753459647|                          Brenda Walpole|       32|
    |I Blink: And Other Questions About My...|0753456109|                           Brigid Avison|       32|
    +----------------------------------------+----------+----------------------------------------+---------+
    only showing top 10 rows
    
    

### Sorting in ascending order using Spark Functions

* Recall that Spark has functions in `org.apache.spark.sql.functions` that can be imported and used in a Spark query
* Be sure to [consult the API](https://spark.apache.org/docs/2.3.0/api/scala/index.html#org.apache.spark.sql.functions$) on this functions
* Note: The online docs will not hyperlink very well


```scala
import org.apache.spark.sql.functions

booksDF.select($"title", $"isbn", $"authors", $"num_pages")
       .where(($"num_pages".isNotNull) && ($"num_pages" > 30))
       .sort(asc("num_pages")) //Note this call uses functions
       .show(10, truncate=40)
```

    +----------------------------------------+----------+----------------------------------------+---------+
    |                                   title|      isbn|                                 authors|num_pages|
    +----------------------------------------+----------+----------------------------------------+---------+
    |                                The Nose|0688104649|              Nikolai Gogol-Kevin Hawkes|       31|
    |The Cognitive Style of PowerPoint: Pi...|0961392169|                         Edward R. Tufte|       31|
    |     The Chronicles of Narnia CD Box Set|0694524751|              C.S. Lewis-Kenneth Branagh|       31|
    |               Blueberries for the Queen|0066239427|John  Paterson-Katherine Paterson-Sus...|       32|
    |The Little Mouse  the Red Ripe Strawb...|0859533301|                             Don    Wood|       32|
    |Literature Circle Guide: Bridge to Te...|0439271711|                          Tara MacCarthy|       32|
    |Literature Circle Guide: A Wrinkle in...|043927169X|                          Tara MacCarthy|       32|
    |          Secret of the Peaceful Warrior|0915811235|Dan Millman-Robert D. San Souci-T. Ta...|       32|
    |The Sun Rises: and Other Questions Ab...|0753459647|                          Brenda Walpole|       32|
    |I Blink: And Other Questions About My...|0753456109|                           Brigid Avison|       32|
    +----------------------------------------+----------+----------------------------------------+---------+
    only showing top 10 rows
    
    




    import org.apache.spark.sql.functions
    



### Sorting explicitly in ascending order using `Column`'s `desc` method

* Each `Column` has a `desc` method that can be used with sort to sort in descending order


```scala
import org.apache.spark.sql.functions

booksDF.select($"title", $"isbn", $"authors", $"num_pages")
       .where(($"num_pages".isNotNull) && ($"num_pages" > 30))
       .sort(desc("num_pages"))
       .show(10, truncate=40)
```

    +----------------------------------------+----------+----------------------------------------+---------+
    |                                   title|      isbn|                                 authors|num_pages|
    +----------------------------------------+----------+----------------------------------------+---------+
    |The Complete Aubrey/Maturin Novels (5...|039306011X|                         Patrick O'Brian|     6576|
    |                    The Second World War|039541685X|        Winston S. Churchill-John Keegan|     4736|
    |      In Search of Lost Time (6 Volumes)|0812969642|Marcel Proust-C.K. Scott Moncrieff-An...|     4211|
    |The Norton Anthology of English Liter...|0393928330|M.H. Abrams-Stephen Greenblatt-James ...|     3956|
    |  Remembrance of Things Past (Boxed Set)|0701125594|Marcel Proust-C.K. Scott Moncrieff-Fr...|     3400|
    |Harry Potter Collection (Harry Potter...|0439827604|                            J.K. Rowling|     3342|
    |The Norton Anthology of English Liter...|0393925315|          M.H. Abrams-Stephen Greenblatt|     3072|
    |The Norton Anthology of English Liter...|0393925323|M.H. Abrams-Stephen Greenblatt-James ...|     3072|
    |                Summa Theologica  5 Vols|0870610635|                          Thomas Aquinas|     3020|
    |                  Civil War: a Narrative|0394749138|                            Shelby Foote|     2965|
    +----------------------------------------+----------+----------------------------------------+---------+
    only showing top 10 rows
    
    

### Sorting in descending order using Spark Functions

* Recall that Spark has functions in `org.apache.spark.sql.functions` that can be imported and used in a Spark query
* Note: The `desc` method from the `functions` object only takes a `String`


```scala
booksDF.select($"title", $"isbn", $"authors", $"num_pages")
       .where(($"num_pages".isNotNull) && ($"num_pages" > 30))
       .sort($"num_pages".desc)
       .show(10, truncate=40)
```

### Specialized sorting with nulls

* Both the `Column` and the `functions` objects have to sort `null` values:
  * `nulls_first`
  * `nulls_last` 


```scala
booksDF.select($"title", $"isbn", $"authors", $"num_pages")
       .sort($"num_pages".desc_nulls_first)
       .show(10, truncate=40)
```

    +----------------------------------------+----------+----------------------------------------+---------+
    |                                   title|      isbn|                                 authors|num_pages|
    +----------------------------------------+----------+----------------------------------------+---------+
    |                                    null|      null|                                    null|     null|
    |                                    null|      null|                                    null|     null|
    |                                    null|      null|                                    null|     null|
    |                                    null|      null|                                    null|     null|
    |                                    null|      null|                                    null|     null|
    |The Complete Aubrey/Maturin Novels (5...|039306011X|                         Patrick O'Brian|     6576|
    |                    The Second World War|039541685X|        Winston S. Churchill-John Keegan|     4736|
    |      In Search of Lost Time (6 Volumes)|0812969642|Marcel Proust-C.K. Scott Moncrieff-An...|     4211|
    |The Norton Anthology of English Liter...|0393928330|M.H. Abrams-Stephen Greenblatt-James ...|     3956|
    |  Remembrance of Things Past (Boxed Set)|0701125594|Marcel Proust-C.K. Scott Moncrieff-Fr...|     3400|
    +----------------------------------------+----------+----------------------------------------+---------+
    only showing top 10 rows
    
    


```scala
booksDF.select($"title", $"isbn", $"authors", $"num_pages")
       .sort($"num_pages".asc_nulls_first)
       .show(10, truncate=40)
```

    +----------------------------------------+----------+---------------------------------------+---------+
    |                                   title|      isbn|                                authors|num_pages|
    +----------------------------------------+----------+---------------------------------------+---------+
    |                                    null|      null|                                   null|     null|
    |                                    null|      null|                                   null|     null|
    |                                    null|      null|                                   null|     null|
    |                                    null|      null|                                   null|     null|
    |                                    null|      null|                                   null|     null|
    |              The Summons / The Brethren|0739342770|John Grisham-Michael Beck-Frank  Muller|        0|
    |        The Tragedy of Pudd'nhead Wilson|140015068X|            Mark Twain-Michael Prichard|        0|
    |The Clan of the Cave Bear  Part 1 of ...|0736606378|            Jean M. Auel-Donada  Peters|        0|
    |                The Lady and the Unicorn|0965903532|                        Tracy Chevalier|        0|
    |  The Da Vinci Code (Robert Langdon  #2)|0739339788|                 Dan Brown-Paul Michael|        0|
    +----------------------------------------+----------+---------------------------------------+---------+
    only showing top 10 rows
    
    


```scala
import org.apache.spark.sql.functions

booksDF.select($"title", $"isbn", $"authors", $"num_pages")
       .sort(desc_nulls_first("num_pages"))
       .show(10, truncate=40)
```

    +----------------------------------------+----------+----------------------------------------+---------+
    |                                   title|      isbn|                                 authors|num_pages|
    +----------------------------------------+----------+----------------------------------------+---------+
    |                                    null|      null|                                    null|     null|
    |                                    null|      null|                                    null|     null|
    |                                    null|      null|                                    null|     null|
    |                                    null|      null|                                    null|     null|
    |                                    null|      null|                                    null|     null|
    |The Complete Aubrey/Maturin Novels (5...|039306011X|                         Patrick O'Brian|     6576|
    |                    The Second World War|039541685X|        Winston S. Churchill-John Keegan|     4736|
    |      In Search of Lost Time (6 Volumes)|0812969642|Marcel Proust-C.K. Scott Moncrieff-An...|     4211|
    |The Norton Anthology of English Liter...|0393928330|M.H. Abrams-Stephen Greenblatt-James ...|     3956|
    |  Remembrance of Things Past (Boxed Set)|0701125594|Marcel Proust-C.K. Scott Moncrieff-Fr...|     3400|
    +----------------------------------------+----------+----------------------------------------+---------+
    only showing top 10 rows
    
    




    import org.apache.spark.sql.functions
    




```scala
import org.apache.spark.sql.functions

booksDF.select($"title", $"isbn", $"authors", $"num_pages")
       .sort(asc_nulls_first("num_pages"))
       .show(10, truncate=40)
```

    +----------------------------------------+----------+---------------------------------------+---------+
    |                                   title|      isbn|                                authors|num_pages|
    +----------------------------------------+----------+---------------------------------------+---------+
    |                                    null|      null|                                   null|     null|
    |                                    null|      null|                                   null|     null|
    |                                    null|      null|                                   null|     null|
    |                                    null|      null|                                   null|     null|
    |                                    null|      null|                                   null|     null|
    |              The Summons / The Brethren|0739342770|John Grisham-Michael Beck-Frank  Muller|        0|
    |        The Tragedy of Pudd'nhead Wilson|140015068X|            Mark Twain-Michael Prichard|        0|
    |The Clan of the Cave Bear  Part 1 of ...|0736606378|            Jean M. Auel-Donada  Peters|        0|
    |                The Lady and the Unicorn|0965903532|                        Tracy Chevalier|        0|
    |  The Da Vinci Code (Robert Langdon  #2)|0739339788|                 Dan Brown-Paul Michael|        0|
    +----------------------------------------+----------+---------------------------------------+---------+
    only showing top 10 rows
    
    




    import org.apache.spark.sql.functions
    



## `groupBy` and aggregate functions

* `groupBy` groups by one or more columns
* `groupBy` returns _always_ a `org.apache.spark.sql.RelationalGroupedDataset`
* From the `RelationalGroupedDataset` you can call any what are called _Aggregate Functions_

### Setting up data before the `groupBy`

First some setup, it seems that authors are split by a dash `-`, so let's do a split, and get the first item and rename the column `primary_author`. We will also select the `title`, `average_rating`, `ratings_count` and the `num_pages`


```scala
val booksByPrimaryAuthor = booksDF.select((split(col("authors"), "-").getItem(0).as("primary_author")), 
                                           col("title"), 
                                           col("average_rating"),
                                           col("ratings_count"),
                                           col("num_pages"))
booksByPrimaryAuthor.show(10, truncate=30)
```

    +----------------------+------------------------------+--------------+-------------+---------+
    |        primary_author|                         title|average_rating|ratings_count|num_pages|
    +----------------------+------------------------------+--------------+-------------+---------+
    |          J.K. Rowling|Harry Potter and the Half-B...|          4.56|      1944099|      652|
    |          J.K. Rowling|Harry Potter and the Order ...|          4.49|      1996446|      870|
    |          J.K. Rowling|Harry Potter and the Sorcer...|          4.47|      5629932|      320|
    |          J.K. Rowling|Harry Potter and the Chambe...|          4.41|         6267|      352|
    |          J.K. Rowling|Harry Potter and the Prison...|          4.55|      2149872|      435|
    |          J.K. Rowling|Harry Potter Boxed Set  Boo...|          4.78|        38872|     2690|
    |W. Frederick Zimmerman|Unauthorized Harry Potter B...|          3.69|           18|      152|
    |          J.K. Rowling|Harry Potter Collection (Ha...|          4.73|        27410|     3342|
    |         Douglas Adams|The Ultimate Hitchhiker's G...|          4.38|         3602|      815|
    |         Douglas Adams|The Ultimate Hitchhiker's G...|          4.38|       240189|      815|
    +----------------------+------------------------------+--------------+-------------+---------+
    only showing top 10 rows
    
    




    booksByPrimaryAuthor: org.apache.spark.sql.DataFrame = [primary_author: string, title: string ... 3 more fields]
    



### Doing the grouping

* Notice in the response, this is returning a `org.apache.spark.sql.RelationalGroupedDataset`
* Once established we can run any of the aggregate functions from the [RelationalGroupedDataset API](https://spark.apache.org/docs/2.3.0/api/scala/index.html#org.apache.spark.sql.RelationalGroupedDataset)


```scala
import org.apache.spark.sql.RelationalGroupedDataset
val groupDataset:RelationalGroupedDataset = booksByPrimaryAuthor.groupBy("primary_author")
```




    import org.apache.spark.sql.RelationalGroupedDataset
    groupDataset: org.apache.spark.sql.RelationalGroupedDataset = RelationalGroupedDataset: [grouping expressions: [primary_author: string], value: [primary_author: string, title: string ... 3 more fields], type: GroupBy]
    




```scala
val averageRatingsByAuthor = groupDataset.avg("average_rating")
averageRatingsByAuthor.show(10)
```

    +--------------------+-------------------+
    |      primary_author|avg(average_rating)|
    +--------------------+-------------------+
    |          James Frey|  3.630000114440918|
    |     Eric Klinenberg| 3.8399999141693115|
    |     Karen Armstrong|  3.971249997615814|
    |                Éric|                3.5|
    |          Dava Sobel| 3.8925000429153442|
    |        Helena Grice|  3.700000047683716|
    |         Ann Rinaldi| 3.7899999618530273|
    |         Ann Beattie|  3.440000057220459|
    |Brian Michael Bendis| 3.8899999856948853|
    |Michael Eliot Howard|  4.050000190734863|
    +--------------------+-------------------+
    only showing top 10 rows
    
    




    averageRatingsByAuthor: org.apache.spark.sql.DataFrame = [primary_author: string, avg(average_rating): double]
    



### Cleaning up the results

This is kind of ugly and doesn't give us what we are looking for, let us do the following:
* Clean up the `avg` column by rounding 
* Capture the top 10
* Renaming the column 
* Sorting by descendingly


```scala
val averageRatingsByAuthor2 = averageRatingsByAuthor.withColumn("average_author_rating", $"avg(average_rating)").drop($"avg(average_rating)")
averageRatingsByAuthor2.show(10)
```

    +--------------------+---------------------+
    |      primary_author|average_author_rating|
    +--------------------+---------------------+
    |          James Frey|    3.630000114440918|
    |     Eric Klinenberg|   3.8399999141693115|
    |     Karen Armstrong|    3.971249997615814|
    |                Éric|                  3.5|
    |          Dava Sobel|   3.8925000429153442|
    |        Helena Grice|    3.700000047683716|
    |         Ann Rinaldi|   3.7899999618530273|
    |         Ann Beattie|    3.440000057220459|
    |Brian Michael Bendis|   3.8899999856948853|
    |Michael Eliot Howard|    4.050000190734863|
    +--------------------+---------------------+
    only showing top 10 rows
    
    




    averageRatingsByAuthor2: org.apache.spark.sql.DataFrame = [primary_author: string, average_author_rating: double]
    




```scala
val averageRatingsByAuthorDesc = averageRatingsByAuthor2.sort($"average_author_rating".desc)
averageRatingsByAuthorDesc.show(10)
```

    +--------------------+---------------------+
    |      primary_author|average_author_rating|
    +--------------------+---------------------+
    |        Ross Garnaut|                  5.0|
    |      Chris    Green|                  5.0|
    |     Aaron Rosenberg|                  5.0|
    |     James E. Ingram|                  5.0|
    |       Chris Jiggins|                  5.0|
    |     Julie Sylvester|                  5.0|
    |      Laura Driscoll|                  5.0|
    |       John  Diamond|                  5.0|
    |     Svetlana Alpers|                  5.0|
    |Middlesex Borough...|                  5.0|
    +--------------------+---------------------+
    only showing top 10 rows
    
    




    averageRatingsByAuthorDesc: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [primary_author: string, average_author_rating: double]
    



#### Doesn't seem right

The above data doesn't seem right, one review with "5.0" shouldn't outweigh "4.2" with 1000s of reviews. Let's start with the grouping again, and add some more aggregations.

In Spark, the `RelationalGroupedDataset` has a way to aggregate more than one column, and it is the aggregate method. We can use it in combination with with the `org.apache.spark.sql.functions`

If you are consulting the `org.apache.spark.sql.functions` API, look for the _Aggregate Functions_ section


```scala
import org.apache.spark.sql.functions._
val authorAggregate = groupDataset.agg(avg("average_rating").as("average_author_rating"),
                                       sum("ratings_count").as("total_ratings"))
authorAggregate.show(10)
```

    +--------------------+---------------------+-------------+
    |      primary_author|average_author_rating|total_ratings|
    +--------------------+---------------------+-------------+
    |          James Frey|    3.630000114440918|       195863|
    |     Eric Klinenberg|   3.8399999141693115|          674|
    |     Karen Armstrong|    3.971249997615814|        67247|
    |                Éric|                  3.5|         2080|
    |          Dava Sobel|   3.8925000429153442|        67718|
    |        Helena Grice|    3.700000047683716|           10|
    |         Ann Rinaldi|   3.7899999618530273|         4988|
    |         Ann Beattie|    3.440000057220459|         1174|
    |Brian Michael Bendis|   3.8899999856948853|         1665|
    |Michael Eliot Howard|    4.050000190734863|          190|
    +--------------------+---------------------+-------------+
    only showing top 10 rows
    
    




    import org.apache.spark.sql.functions._
    authorAggregate: org.apache.spark.sql.DataFrame = [primary_author: string, average_author_rating: double ... 1 more field]
    



### Performing our calculation


```scala
val authorAggregateWeighed = authorAggregate
                                .withColumn("sum_product_ratings", $"total_ratings" * $"average_author_rating")
authorAggregateWeighed.show(10)
```

    +--------------------+---------------------+-------------+-------------------+
    |      primary_author|average_author_rating|total_ratings|sum_product_ratings|
    +--------------------+---------------------+-------------+-------------------+
    |          James Frey|    3.630000114440918|       195863|  710982.7124147415|
    |     Eric Klinenberg|   3.8399999141693115|          674|  2588.159942150116|
    |     Karen Armstrong|    3.971249997615814|        67247| 267054.64858967066|
    |                Éric|                  3.5|         2080|             7280.0|
    |          Dava Sobel|   3.8925000429153442|        67718|  263592.3179061413|
    |        Helena Grice|    3.700000047683716|           10|  37.00000047683716|
    |         Ann Rinaldi|   3.7899999618530273|         4988|   18904.5198097229|
    |         Ann Beattie|    3.440000057220459|         1174|  4038.560067176819|
    |Brian Michael Bendis|   3.8899999856948853|         1665|  6476.849976181984|
    |Michael Eliot Howard|    4.050000190734863|          190|   769.500036239624|
    +--------------------+---------------------+-------------+-------------------+
    only showing top 10 rows
    
    




    authorAggregateWeighed: org.apache.spark.sql.DataFrame = [primary_author: string, average_author_rating: double ... 2 more fields]
    



### Show the most prolific authors

* Here we will limit the results just to the top 10


```scala
val topAuthors = authorAggregateWeighed
                    .select($"primary_author", $"average_author_rating")
                    .sort($"sum_product_ratings".desc)
                    .limit(10)
topAuthors.show()

```

    +-------------------+---------------------+
    |     primary_author|average_author_rating|
    +-------------------+---------------------+
    |       J.K. Rowling|    4.512857096535819|
    |     J.R.R. Tolkien|    4.235945927130209|
    |       Stephen King|    4.009765664115548|
    |William Shakespeare|   3.9309734812879986|
    |          Dan Brown|    3.799130460490351|
    |    Stephenie Meyer|   3.5899999141693115|
    |    Nicholas Sparks|    3.997692291553204|
    | George R.R. Martin|    4.148571389062064|
    |      J.D. Salinger|   3.9745454137975518|
    |      George Orwell|    4.166111177868313|
    +-------------------+---------------------+
    
    




    topAuthors: org.apache.spark.sql.Dataset[org.apache.spark.sql.Row] = [primary_author: string, average_author_rating: double]
    


