from fastapi import FastAPI
from fastapi.responses import JSONResponse
import geopandas
import io
from PIL import Image
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
import base64
from fastapi.middleware.cors import CORSMiddleware
from pyspark.sql.functions import max, min, col
import pandas as pd
from shapely.geometry import Point
import math

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

spark = SparkSession \
    .builder \
    .appName("HawaiiMap") \
    .master('local')\
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1')\
    .config('spark.driver.memory','8g')\
    .getOrCreate()

@app.get("/earthquakeMap")
async def earthquakeMap():
    stateMap = geopandas.read_file("Datasets/Hawaii_State_House_Districts_2022.geojson").to_crs(epsg=3857)

    earthquakeDf = spark\
        .read\
        .option("inferSchema", "true")\
        .option("header", "true")\
        .csv("Datasets/Earthquakes/*.csv")
    
    seismicDf = spark\
        .read\
        .option("inferSchema", "true")\
        .option("header", "true")\
        .csv("Datasets/Seismic  Activity/*.csv")

    seismicDf = seismicDf.select('latitude', 'longitude', 'mag')
    earthquakeDf = earthquakeDf.select('latitude', 'longitude', 'mag')
    earthquakeDf = earthquakeDf.union(seismicDf)

    #min and max values for normalization
    maxV = earthquakeDf.select(max(earthquakeDf.mag).alias("max")).first()[0]
    minV = earthquakeDf.select(min(earthquakeDf.mag).alias("min")).first()[0]

    earthquakeDf = earthquakeDf.withColumn('distribution', (col('mag') - minV) / (maxV - minV))\
        .select('latitude', 'longitude', 'mag', 'distribution')

    earthquakeDf = earthquakeDf.toPandas()

    earthquakeDf['geometry'] = earthquakeDf.apply(lambda point: Point(point['longitude'], point['latitude']), axis=1)
    earthquakeDf = geopandas.GeoDataFrame(earthquakeDf, geometry='geometry', crs="EPSG:4326")
    earthquakeDf = earthquakeDf.to_crs(epsg=3857)
    #buffer is in meters
    #formula for calculating the buffer taken from the below post on stackexchange
    #https://gis.stackexchange.com/questions/221931/calculate-radius-from-magnitude-of-earthquake-on-leaflet-map
    earthquakeDf['geometry'] = earthquakeDf.apply(lambda df: df['geometry'].buffer((math.exp(df['mag']/1.01-0.13))*1000), axis=1)

    #join earthquake areas from earthquakeDf into finalMap polygons they fall into
    finalMap = geopandas.sjoin(earthquakeDf, stateMap, how="inner", predicate='intersects')

    aggregateDf = finalMap.groupby(finalMap.index)['distribution'].mean().reset_index()

    stateMap = stateMap.merge(aggregateDf, left_index=True, right_on='index', how='left')

    #replace N/A values with the mean
    mean = stateMap['distribution'].mean()
    stateMap.fillna({'distribution':mean}, inplace=True)

    stateMap.plot(column='distribution', cmap='Reds')

    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()
    img = base64.b64encode(buf.getvalue()).decode('utf-8')

    return JSONResponse(content={"img": img})

@app.get("/wildfireMap")
async def wildFireMap():
    stateMap = geopandas.read_file("Datasets/Hawaii_State_House_Districts_2022.geojson").to_crs(epsg=3857)
    
    wildfireDf = spark\
        .read\
        .option("inferSchema", "true")\
        .option("header", "true")\
        .csv("Datasets/Wildfires/*.csv")

    wildfireDf = wildfireDf.withColumn('mag', wildfireDf['Acres'].cast('double'))\
    .withColumn('latitude', wildfireDf['Latitude'].cast('float'))\
    .withColumn('longitude', wildfireDf['Longitude'].cast('float'))\
    .select('latitude', 'longitude', 'mag')
    
    maxVF = wildfireDf.select(max(wildfireDf.mag).alias("max")).first()[0]
    minVF = 0

    wildfireDf = wildfireDf.withColumn('distribution', (col('mag') - minVF) / (maxVF - minVF))\
        .select('latitude', 'longitude', 'distribution')
    
    wildfireDf = wildfireDf.toPandas()

    wildfireDf['geometry'] = wildfireDf.apply(lambda point: Point(point['longitude'], point['latitude']), axis=1)
    wildfireDf = geopandas.GeoDataFrame(wildfireDf, geometry='geometry', crs="EPSG:4326")
    wildfireDf = wildfireDf.to_crs(epsg=3857)

    #join earthquake areas from earthquakeDf into finalMap polygons they fall into
    filledMap = geopandas.sjoin(wildfireDf, stateMap, how="inner", predicate='within')

    aggregateDf = filledMap.groupby(filledMap.index)['distribution'].mean().reset_index()

    finalMap = stateMap.merge(aggregateDf, left_index=True, right_on='index', how='left')

    #replace N/A values with the mean
    mean = finalMap['distribution'].mean()
    finalMap.fillna({'distribution':mean}, inplace=True)

    finalMap.plot(column='distribution', cmap='Reds')

    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()
    img = base64.b64encode(buf.getvalue()).decode('utf-8')

    return JSONResponse(content={"img": img})
