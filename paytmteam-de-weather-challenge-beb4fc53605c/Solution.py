#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyspark
from pyspark.sql import SparkSession


# In[129]:


from pyspark.sql import functions as f
from pyspark.sql.window import Window
from pyspark.sql.types import *


# In[2]:


spark = SparkSession.builder             .getOrCreate()


# In[4]:


### Step 1 - Settng Up the Data ###


# In[54]:


# 1. Load the global weather data into your big data technology of choice.

# Load Country List
country_list = spark.read.format("csv")         .option("header", "true")         .load(f"countrylist.csv")

# Station Lists
station_list = spark.read.format("csv")     .option("header", "true")     .load(f"stationlist.csv")

# Global Weather
global_weather = spark.read.format("csv")     .option("header", "true")     .load(f"./data/2019")     .withColumnRenamed('STN---', 'STN')


# In[55]:


country_list_suf = country_list.select(*[f.col(column).alias(f"{column}_CL") for column in country_list.columns])
station_list_suf = station_list.select(*[f.col(column).alias(f"{column}_SL") for column in station_list.columns])


# In[56]:


# 2. Join the stationlist.csv with the countrylist.csv to get the full 
# country name for each station number.
full_country_name = country_list_suf     .join(station_list_suf, country_list_suf.COUNTRY_ABBR_CL == station_list_suf.COUNTRY_ABBR_SL,
          'left') \
    .select(*['STN_NO_SL', 'COUNTRY_FULL_CL', 'COUNTRY_ABBR_CL', 'COUNTRY_ABBR_SL'])

full_country_name.show(n = 5)


# In[58]:


# 3. Join the global weather data with the full country names by station number.
# Leaking : 4158416 (Weather) versus 4156323 After Join
weather_by_station = full_country_name     .join(global_weather, full_country_name.STN_NO_SL == global_weather.STN, 'left')


# In[77]:


weather_by_station.printSchema()


# In[106]:


### Step 2 - Questions ###

# NOTE : STATIONs without a Corresponding STN_NO in 'station_list' were intentionally left out...

# 1. Which country had the hottest average mean temperature over the year? # DJIBOUTI
weather_by_station     .withColumn("TEMP", weather_by_station['TEMP'].cast(FloatType()))     .filter(~f.col("TEMP").isNull())     .groupby(*['COUNTRY_FULL_CL', ])     .agg({"TEMP" : "avg"})     .select(*['COUNTRY_FULL_CL', f.col('avg(TEMP)').alias('TEMP'), ])     .orderBy(f.desc('TEMP')).show(n = 1)


# In[135]:


# 2. Which country had the most consecutive days of tornadoes/funnel cloud formations?

# NOTE : Incomplete

dates_for_2019 = weather_by_station     .select([f.to_date(f.col('YEARMODA'), 'yyyyMMdd')])     .distinct()

window = Window.partitionBy('COUNTRY_FULL_CL').orderBy('YEARMODA')

weather_by_station_tornado_funnel = weather_by_station     .select([f.to_date(f.col('YEARMODA'), 'yyyyMMdd').alias('YEARMODA'), 'FRSHTT', 'COUNTRY_FULL_CL', ])     .filter(f.col('FRSHTT').endswith('1').alias('FRSHTT'))     .select(*['COUNTRY_FULL_CL', 'YEARMODA', 'FRSHTT', ])     .distinct()     .orderBy(*['COUNTRY_FULL_CL', 'YEARMODA', 'FRSHTT'], asc = [True, True, True, ])     .select(*['COUNTRY_FULL_CL', 'YEARMODA', 'FRSHTT', f.lag('YEARMODA', 1).over(window).alias('PREV_YEARMODA')])


# In[136]:


weather_by_station_tornado_funnel.filter(f.col('PREV_YEARMODA').isNotNull() == True).count()


# In[109]:


# 3. Which country had the second highest average mean wind speed over the year?
weather_by_station     .withColumn("WDSP", weather_by_station['WDSP'].cast(FloatType()))     .filter(~f.col('WDSP').isNull())     .groupby(*['COUNTRY_FULL_CL', ])     .agg({"WDSP" : "avg"})     .select(*['COUNTRY_FULL_CL', f.col('avg(WDSP)').alias('WDSP')])     .orderBy(f.desc("WDSP"))     .show(n = 2)


# In[10]:


#### Data Discovery ####
# 288
country_list.count()
# 288
country_list.select(*['COUNTRY_ABBR', ]).distinct().count()


# In[11]:


# 25 297
station_list.select(*['STN_NO', ]).distinct().count()
# 25 306
station_list.count()


# In[47]:


full_country_name     .groupby(*['STN_NO_SL', ])     .agg(f.countDistinct(f.col('COUNTRY_ABBR_SL')))     .orderBy(*[f.col('count(COUNTRY_ABBR_SL)').alias('COUNTRY_ABBR_SL'), ], asc = [False])     .take(5)


# In[30]:


global_weather.take(5)


# In[ ]:


weather_by_station.take(2)


# In[ ]:





# In[ ]:




