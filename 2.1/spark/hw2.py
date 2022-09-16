from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import SparkSession
from pyspark.sql.functions import col



spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()


# 1 


# Создаем схему данных
schema = StructType([
    StructField('row_id', IntegerType(), True),
    StructField('discipline', StringType(), True),
    StructField('season', StringType(), True)
])

df1 = spark.createDataFrame([(1, 'Фигурное катание', 'winter'),
                            (2, 'Хоккей', 'winter'),
                            (3, 'Биатлон ', 'winter'),
                            (4, 'Шорттрек', 'winter'),
                            (5, 'Керлинг', 'winter'),
                            (6, 'Футбол', 'summer'),
                            (7, 'Волейбол', 'summer'),
                            (8, 'Баскетбол', 'summer'),
                            (9, 'Велоспорт', 'summer'),
                            (10, 'Прыжки в длину', 'summer')
                            ], schema)

df1.show()

# Записываем в файл
df1.repartition(1).write.options(header=True).csv("cc_out.csv", sep='|')


# 2


schema = StructType() \
      .add("Name",StringType(),True) \
      .add("NOC",StringType(),True) \
      .add("Discipline",StringType(),True)
      
df2 = spark.read.format("csv") \
      .option("header", True) \
      .option("delimiter", ';') \
      .schema(schema) \
      .load("/home/sag163/airflow/dags/spark/Athletes.csv")

print("Количество строк в df2:", df2.count())

# Групировка данных

grouped2 = df2.groupBy(['Discipline']).count()

# Сохраняем результат
df2.write.parquet("parquet1/") 


# 3 

schema = StructType() \
      .add("Name",StringType(),True) \
      .add("NOC",StringType(),True) \
      .add("Discipline",StringType(),True)
      
df2 = spark.read.format("csv") \
      .option("header", True) \
      .option("delimiter", ';') \
      .schema(schema) \
      .load("/home/sag163/airflow/dags/spark/Athletes.csv")

# Групировка данных

grouped1 = df1.groupBy(['Discipline']).count()

grouped2 = df2.groupBy(['Discipline']).count()

# Объеденияем датафреймы
df3 = grouped1.union(grouped2)
print('df3')
df3.show(100)

# Получаем список дисциплин

lst = [(row.Discipline) for row in grouped1.select('Discipline').collect()]
print('Список дисциплин', lst)
# Выбираем дисциплины
df4 = df3.where(col("Discipline").isin(lst))
print('df4')
df4.show()


# записываем в parquet
df4.write.parquet("parquet2/") 


