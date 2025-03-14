
Задание:
Описать или реализовать использование Apache Spark под управлением YARN
для чтения, трансформации и записи данных, решение должно включать в себя:
Запуск сессии Apache Spark под управлением YARN, развернутого на кластере из предыдущих заданий
Подключение к кластеру HDFS развернутому в предыдущих заданиях
Использование созданной ранее сессии Spark для чтения данных, которые были предварительно загружены на HDFS
Применение нескольких трансформаций данных (например, агрегаций или преобразований типов)
Применение партиционирования при сохранении данных
Сохранение преобразованных данных как таблицы
Проверку возможности чтения данных стандартным клиентом hive

Данные:
узел для входа 176.109.81.242 
jn 192.168.1.106 
nn 192.168.1.107 
dn-00 192.168.1.109 
dn-01 192.168.1.108

Пререквизиты: выполнены инструкции из предыдущих заданий, установленные python venv и pip (можно установить командами ```sudo apt install python3-venv```, ```sudo apt install python3-pip```), запущенный metastore.

Подключаемся к ноде:
```
ssh team@176.109.81.242
```

перейдем на пользователя hadoop:
```
sudo -i -u hadoop
```

Установим python venv и pip:
```
sudo apt install python3-venv
```
```
sudo apt install python3-pip
```

Скачаем Spark:
```
wget https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz
```
И распакуем его:
```
tar -xzvf spark-3.5.3-bin-hadoop3.tgz
```

Выполним следующие команды:
```
export HADOOP_CONF_DIR="/home/hadoop/hadoop-3.4.0/etc/hadoop"
```
```
export HIVE_HOME="/home/hadoop/apache-hive-4.0.1-bin"
```
```
export HIVE_CONF_DIR=$HIVE_HOME/conf
```
```
export HIVE_AUX_JARS_PATH=$HIVE_HOME/lib/*
```
```
export PATH=$PATH:$HIVE_HOME/bin
```
```
export SPARK_LOCAL_IP=192.168.1.106
```
```
export SPARK_DIST_CLASSPATH="/home/hadoop/spark-3.5.3-bin-hadoop3/jars/*:/home/hadoop/hadoop-3.4.0/etc/hadoop:/home/hadoop/hadoop-3.4.0/share/hadoop/common/lib/*:/home/hadoop/hadoop-3.4.0/share/hadoop/common/*:/home/hadoop/hadoop-3.4.0/share/hadoop/hdfs:/home/hadoop/hadoop-3.4.0/share/hadoop/hdfs/lib/*:/home/hadoop/hadoop-3.4.0/share/hadoop/hdfs/*:/home/hadoop/hadoop-3.4.0/share/hadoop/mapreduce/*:/home/hadoop/hadoop-3.4.0/share/hadoop/yarn:/home/hadoop/hadoop-3.4.0/share/hadoop/yarn/lib/*:/home/hadoop/hadoop-3.4.0/share/hadoop/yarn/*:/home/hadoop/apache-hive-4.0.0-alpha-2-bin/*:/home/hadoop/apache-hive-4.0.0-alpha-2-bin/lib/*"
```
Перейдем в папку с дистрибутивом Spark:
```
cd spark-3.5.3-bin-hadoop3/
```
И объявим переменную SPARK_HOME:
```
export SPARK_HOME=$(pwd)
```

Объявим переменную PYTHONPATH и добавим путь Spark в PATH:
```
export PYTHONPATH=$(ZIPS=("$SPARK_HOME"/python/lib/*.zip); IFS=:; echo "${ZIPS[*]}"):$PYTHONPATH
```
```
export PATH=$SPARK_HOME/bin:$PATH
```
Вернемся на директорию назад:
```
cd ../
```

Создадим новое окружение:
```
python3 -m venv venv
```

Активируем его:
```
source venv/bin/activate
```

Установим pip
```
pip install -U pip
```

Установим ipython:
```
pip install ipython
```

Установим onetl:
```
pip install onetl[files]
```

Теперь можно (при наличии) положить данные на hadoop, с которыми будем работать:
```
hdfs dfs -put for_spark.csv /input
```

Войдем в интерактивную оболочку python:
```
ipython
```
импортируем SparkSession из pyspark.sql:
```
from pyspark.sql import SparkSession
```
Импортируем модуль функций:
```
from pyspark.sql import functions as F
```
Импортируем вспомогательные модули:
```
from onetl.connection import SparkHDFS
```
```
from onetl.connection import Hive
```
```
from onetl.file import FileDFReader
```
```
from onetl.file.format import CSV
```
```
from onetl.db import DBWriter
```

Теперь можно создать сессию:
```
spark = SparkSession.builder \
    .master("yarn") \
    .appName("spark-with-yarn") \
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
    .config("spark.hive.metastore.uris", "thrift://tmpl-jn:5432") \
    .enableHiveSupport() \
    .getOrCreate()
```
Подключимся к HDFS:
```
hdfs = SparkHDFS(host="tmpl-nn", port=9000, spark=spark, cluster="test")
```
Можно проверить соединение командной ```hdfs.check()```

Создадим объект reader:
```
reader = FileDFReader(connection=hdfs, format=CSV(delimiter=",", header=True), source_path="/input")
```

Читаем подготовленный файл:
```
df = reader.run(["for_spark.csv"])
```

Узнаем количество записей в этом файле:
```
df.count()
```
Можно посмотреть на схему:
```
df.printSchema()
```
Можем посмотреть на количество партиций, которые сейчас есть в Spark:
```
df.rdd.getNumPartitions()
```

Создадим подключение к Hive:
```
hive = Hive(spark=spark, cluster="test")
```
Попробуем записать таблицу в это подключение:
```
writer = DBWriter(connection=hive, table="test.spark_partitions", options={"if_exists": "replace_entire_table"})
```
```
writer.run(df)
```

Применим трансформации данных (на примере выдуманных столбцов, реальные зависят от конкретных данных):
```
df_transformed = df.withColumn("some_column_int", F.col("some_column").cast("integer")) \
                   .groupBy("group_column") \
                   .agg(F.avg("some_column_int").alias("avg_value"))
```
Применим партицирование при сохранении данных:
```
df_transformed.write \
    .mode("overwrite") \
    .partitionBy("group_column") \
    .saveAsTable("test.spark_partitions_transformed")
```
Дальше можно проверить возможность чтения данных стандартным клиентом Hive. Для этого нужно выйти из python:
```
quit()
```
И запустить hive, а затем выполнить любой SQL-запрос
