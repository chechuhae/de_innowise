#!/usr/bin/env python
# coding: utf-8

# In[103]:


import findspark


# In[104]:


findspark.init()
findspark.find()


# In[105]:


from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import col, desc
from pyspark.sql.functions import rank, count
from pyspark.sql.window import Window

appName = "PySpark PostgreSQL Example - via psycopg2"
master = "local"

spark = SparkSession.builder.master(master).appName(appName).getOrCreate()


# In[106]:


url = "jdbc:postgresql://localhost:5432/pagila"
df =  spark.read.format("jdbc")\
    .option("url", url)\
    .option("user", 'postgres')\
    .option("password", '52845466')\
    .option("dbtable", 'information_schema.tables')\
    .option("driver", "org.postgresql.Driver") \
.load()


# In[107]:


df.show(5)


# In[110]:


b_tolist=df.rdd.map(lambda x: x['table_name']).collect()
print(b_tolist)


# # вывести количество фильмов в каждой категории, отсортировать по убыванию.

# In[111]:


url = "jdbc:postgresql://localhost:5432/pagila"
film_list =  spark.read.format("jdbc")\
    .option("url", url)\
    .option("user", 'postgres')\
    .option("password", '52845466')\
    .option("dbtable", 'film_list')\
    .option("driver", "org.postgresql.Driver") \
.load()


# In[112]:


film_list.show()


# In[113]:


film_list.groupBy('category').agg(functions.count('category').alias('quantity'))                              .sort(functions.desc('quantity'))                              .show()


# # вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию

# In[114]:


film_actor =  spark.read.format("jdbc")\
    .option("url", url)\
    .option("user", 'postgres')\
    .option("password", '52845466')\
    .option("dbtable", 'film_actor')\
    .option("driver", "org.postgresql.Driver") \
.load()

# In[115]:


actor =  spark.read.format("jdbc")\
    .option("url", url)\
    .option("user", 'postgres')\
    .option("password", '52845466')\
    .option("dbtable", 'actor')\
    .option("driver", "org.postgresql.Driver") \
.load()
# In[120]:


film =  spark.read.format("jdbc")\
    .option("url", url)\
    .option("user", 'postgres')\
    .option("password", '52845466')\
    .option("dbtable", 'film')\
    .option("driver", "org.postgresql.Driver") \
.load()

# In[162]:


actor_film = film.join(film_actor, 'film_id', 'inner').join(actor, 'actor_id', 'inner')


# In[159]:


actor_film.groupBy('first_name', 'last_name')\
          .agg(functions.sum('rental_duration').alias('q_days'))\
          .orderBy(functions.desc('q_days'))\
          .show(10) 


# # вывести категорию фильмов, на которую потратили больше всего денег

# In[174]:


cat_ac_film = film_list.join(film, film_list.fid == film.film_id, 'inner')



# In[177]:


cat_ac_film.groupBy('category')\
           .agg(functions.sum(col('rental_duration') * col('rental_rate')).alias('total_sum'))\
           .orderBy(desc('total_sum'))\
           .show(1)


# # вывести названия фильмов, которых нет в inventory. Написать запрос без использования оператора IN

# In[178]:


inventory =  spark.read.format("jdbc")\
    .option("url", url)\
    .option("user", 'postgres')\
    .option("password", '52845466')\
    .option("dbtable", 'inventory')\
    .option("driver", "org.postgresql.Driver") \
.load()


# In[179]:


film_inventory = film.join(inventory, 'film_id', 'left')


# In[186]:


film_inventory.select('title').filter(col('inventory_id').isNull()).show()


# # вывести топ 3 актеров, которые больше всего появлялись в фильмах в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех

# In[201]:




# In[218]:


list_actor_film = film_list.join(film_actor, film_list.fid == film_actor.film_id, 'inner')\
                           .join(actor, 'actor_id', 'inner')


# In[220]:


for_rank = list_actor_film.filter(col('category') == 'Children')\
                          .groupBy('first_name', 'last_name')\
                          .agg(count('fid').alias('q_films'))


# In[221]:


WindowSpec = Window.orderBy(desc('q_films'))


# In[223]:


for_rank.withColumn("rank",rank().over(WindowSpec)).filter(col('rank') <=3).show()


# # вывести города с количеством активных и неактивных клиентов (активный — customer.active = 1). Отсортировать по количеству неактивных клиентов по убыванию

# In[268]:


customer =  spark.read.format("jdbc")\
    .option("url", url)\
    .option("user", 'postgres')\
    .option("password", '52845466')\
    .option("dbtable", 'customer')\
    .option("driver", "org.postgresql.Driver") \
.load()


# In[269]:


customer_list =  spark.read.format("jdbc")\
    .option("url", url)\
    .option("user", 'postgres')\
    .option("password", '52845466')\
    .option("dbtable", 'customer_list')\
    .option("driver", "org.postgresql.Driver") \
.load()


# In[281]:


customer_city = customer.join(customer_list, customer.customer_id == customer_list.id, 'inner')
one_df = customer_city.filter(col('active') == 1)
zero_df = customer_city.filter(col('active') == 0)


# In[319]:


one_df.groupBy('city')\
      .agg(functions.count('active').alias('active'))\
      .join(zero_df.groupBy('city')\
                   .agg(functions.count('active').alias('inactive')), 
            'city', 
            'full')\
      .na\
      .fill(0).orderBy(desc('inactive')).show()


# # вывести категорию фильмов, у которой самое большое кол-во часов суммарной аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”. То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.

# In[330]:


inventory =  spark.read.format("jdbc")\
    .option("url", url)\
    .option("user", 'postgres')\
    .option("password", '52845466')\
    .option("dbtable", 'inventory')\
    .option("driver", "org.postgresql.Driver") \
.load()


# In[331]:


rental =  spark.read.format("jdbc")\
    .option("url", url)\
    .option("user", 'postgres')\
    .option("password", '52845466')\
    .option("dbtable", 'rental')\
    .option("driver", "org.postgresql.Driver") \
.load()

# In[332]:


customer =  spark.read.format("jdbc")\
    .option("url", url)\
    .option("user", 'postgres')\
    .option("password", '52845466')\
    .option("dbtable", 'customer')\
    .option("driver", "org.postgresql.Driver") \
.load()

# In[333]:


address =  spark.read.format("jdbc")\
    .option("url", url)\
    .option("user", 'postgres')\
    .option("password", '52845466')\
    .option("dbtable", 'address')\
    .option("driver", "org.postgresql.Driver") \
.load()

# In[334]:


city =  spark.read.format("jdbc")\
    .option("url", url)\
    .option("user", 'postgres')\
    .option("password", '52845466')\
    .option("dbtable", 'city')\
    .option("driver", "org.postgresql.Driver") \
.load()

# In[335]:


end_df = film_list.join(film, film_list.fid == film.film_id)\
                  .join(inventory, 'film_id')\
                  .join(rental, 'inventory_id')\
                  .join(customer, 'customer_id')\
                  .join(address, 'address_id')\
                  .join(city, 'city_id')


# In[360]:


end_df.groupBy('city', 'category')\
      .agg(functions.sum('rental_duration').alias('q_hours'))\
      .filter(col('city').startswith('A'))\
      .orderBy(desc('q_hours')).select('category').show(1)

# In[363]:


end_df.groupBy('city', 'category')\
      .agg(functions.sum('rental_duration').alias('q_hours'))\
      .filter(col('city').contains('-'))\
      .orderBy(desc('q_hours')).select('category').show(1)
