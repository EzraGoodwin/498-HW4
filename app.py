from flask import Flask, request, jsonify
from neo4j import GraphDatabase
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round as spark_round
import math

app = Flask(__name__)

NEO4J_URI      = "bolt://localhost:7687"
NEO4J_USER     = "neo4j"
NEO4J_PASSWORD = "REDACTED_LOL" 
CSV_PATH       = "/home/ezragoodwin/taxi_trips_clean.csv" 

neo4j_driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

spark = SparkSession.builder \
    .appName("TaxiAPI") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

@app.route("/graph-summary")
def graph_summary():
    with neo4j_driver.session() as session:
        result = session.run("""
            MATCH (d:Driver)  WITH count(d) AS driver_count
            MATCH (c:Company) WITH driver_count, count(c) AS company_count
            MATCH (a:Area)    WITH driver_count, company_count, count(a) AS area_count
            MATCH ()-[t:TRIP]->() RETURN driver_count, company_count, area_count, count(t) AS trip_count
        """)
        row = result.single()
        return jsonify({
            "driver_count":  row["driver_count"],
            "company_count": row["company_count"],
            "area_count":    row["area_count"],
            "trip_count":    row["trip_count"]
        })


@app.route("/top-companies")
def top_companies():
    n = int(request.args.get("n", 5))
    with neo4j_driver.session() as session:
        result = session.run("""
            MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[:TRIP]->(:Area)
            RETURN c.name AS name, count(*) AS trip_count
            ORDER BY trip_count DESC
            LIMIT $n
        """, n=n)
        companies = [{"name": r["name"], "trip_count": r["trip_count"]} for r in result]
        return jsonify({"companies": companies})


@app.route("/high-fare-trips")
def high_fare_trips():
    area_id  = int(request.args.get("area_id"))
    min_fare = float(request.args.get("min_fare"))
    with neo4j_driver.session() as session:
        result = session.run("""
            MATCH (d:Driver)-[t:TRIP]->(a:Area {area_id: $area_id})
            WHERE t.fare > $min_fare
            RETURN t.trip_id AS trip_id, t.fare AS fare, d.driver_id AS driver_id
            ORDER BY t.fare DESC
        """, area_id=area_id, min_fare=min_fare)
        trips = [{"trip_id": r["trip_id"], "fare": r["fare"], "driver_id": r["driver_id"]}
                 for r in result]
        return jsonify({"trips": trips})

@app.route("/co-area-drivers")
def co_area_drivers():
    driver_id = request.args.get("driver_id")
    with neo4j_driver.session() as session:
        result = session.run("""
            MATCH (d1:Driver {driver_id: $driver_id})-[:TRIP]->(a:Area)<-[:TRIP]-(d2:Driver)
            WHERE d1 <> d2
            RETURN d2.driver_id AS driver_id, count(DISTINCT a) AS shared_areas
            ORDER BY shared_areas DESC
        """, driver_id=driver_id)
        drivers = [{"driver_id": r["driver_id"], "shared_areas": r["shared_areas"]}
                   for r in result]
        return jsonify({"co_area_drivers": drivers})


@app.route("/avg-fare-by-company")
def avg_fare_by_company():
    with neo4j_driver.session() as session:
        result = session.run("""
            MATCH (c:Company)<-[:WORKS_FOR]-(d:Driver)-[t:TRIP]->(:Area)
            RETURN c.name AS name, round(avg(t.fare), 2) AS avg_fare
            ORDER BY avg_fare DESC
        """)
        companies = [{"name": r["name"], "avg_fare": r["avg_fare"]} for r in result]
        return jsonify({"companies": companies})


@app.route("/area-stats")
def area_stats():
    area_id = int(request.args.get("area_id"))
    df = spark.read.csv(CSV_PATH, header=True, inferSchema=True)

    result = df.filter(col("dropoff_area") == area_id) \
               .groupBy("dropoff_area") \
               .agg(
                   count("*").alias("trip_count"),
                   spark_round(avg("fare"), 2).alias("avg_fare"),
                   spark_round(avg("trip_seconds"), 0).alias("avg_trip_seconds")
               ).collect()

    if not result:
        return jsonify({"area_id": area_id, "trip_count": 0,
                        "avg_fare": 0.0, "avg_trip_seconds": 0})
    r = result[0]
    return jsonify({
        "area_id":          area_id,
        "trip_count":       int(r["trip_count"]),
        "avg_fare":         float(r["avg_fare"]),
        "avg_trip_seconds": int(r["avg_trip_seconds"])
    })


@app.route("/top-pickup-areas")
def top_pickup_areas():
    n = int(request.args.get("n", 5))
    df = spark.read.csv(CSV_PATH, header=True, inferSchema=True)

    result = df.groupBy("pickup_area") \
               .agg(count("*").alias("trip_count")) \
               .orderBy(col("trip_count").desc()) \
               .limit(n) \
               .collect()

    areas = [{"pickup_area": int(r["pickup_area"]),
              "trip_count":  int(r["trip_count"])} for r in result]
    return jsonify({"areas": areas})


@app.route("/company-compare")
def company_compare():
    company1 = request.args.get("company1")
    company2 = request.args.get("company2")

    df = spark.read.csv(CSV_PATH, header=True, inferSchema=True)
    df = df.withColumn("fare_per_minute", col("fare") / (col("trip_seconds") / 60.0))
    df.createOrReplaceTempView("trips")

    result = spark.sql(f"""
        SELECT
            company,
            COUNT(*) AS trip_count,
            ROUND(AVG(fare), 2) AS avg_fare,
            ROUND(AVG(fare_per_minute), 2) AS avg_fare_per_minute,
            ROUND(AVG(trip_seconds), 0) AS avg_trip_seconds
        FROM trips
        WHERE company IN ('{company1}', '{company2}')
        GROUP BY company
        ORDER BY trip_count DESC
    """).collect()

    found = {r["company"] for r in result}
    if company1 not in found or company2 not in found:
        return jsonify({"error": "one or more companies not found"})

    comparison = [{
        "company":             r["company"],
        "trip_count":          int(r["trip_count"]),
        "avg_fare":            float(r["avg_fare"]),
        "avg_fare_per_minute": float(r["avg_fare_per_minute"]),
        "avg_trip_seconds":    int(r["avg_trip_seconds"])
    } for r in result]
    return jsonify({"comparison": comparison})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
