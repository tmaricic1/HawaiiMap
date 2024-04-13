from fastapi import FastAPI
from fastapi.responses import JSONResponse
import geopandas
import io
from PIL import Image
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession
import base64
from fastapi.middleware.cors import CORSMiddleware

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

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/generateMap")
async def generateMap():
    hawaiiMap = geopandas.read_file('Datasets/Hawaii_State_House_Districts_2022.geojson')
    hawaiiMap.plot()
    buf = io.BytesIO()
    plt.savefig(buf, format='png')
    buf.seek(0)
    plt.close()
    img = base64.b64encode(buf.getvalue()).decode('utf-8')

    return JSONResponse(content={"img": img})