from fastapi import FastAPI, HTTPException, Query
from motor.motor_asyncio import AsyncIOMotorClient
from typing import List, Optional
import os
from dotenv import load_dotenv
load_dotenv()

app = FastAPI(title="Logistics Tracking API")

# MongoDB Connection
user = os.getenv('MONGODB_USER')
password = os.getenv('MONGODB_PASSWORD')
uri = f"mongodb+srv://{user}:{password}@mongodb-cluster.nfykfyf.mongodb.net/?appName=mongodb-cluster"
client = AsyncIOMotorClient(uri)
db = client.logistics
trips = db.logistics_truck_data

@app.on_event("startup")
async def startup_db_client():
    try:
        # Send a ping to confirm a successful connection
        await client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(f"Connection failed: {e}")

# Helper to format MongoDB documents
def format_doc(doc) -> dict:
    doc["_id"] = str(doc["_id"])
    return doc

### 1. Advanced Filtering
@app.get("/trips")
async def get_trips(
    status: Optional[str] = None, 
    vehicle_no: Optional[str] = None,
    gps_only: bool = False
):
    """
    Filter by status (e.g., 'Delayed'), vehicle number, 
    or only show records with valid GPS data.
    """
    query = {}
    if status:
        query["status"] = status
    if vehicle_no:
        query["vehicle_no"] = vehicle_no
    if gps_only:
        query["gps_valid"] = True

    cursor = trips.find(query)
    # Mapping _id to string for JSON compatibility
    result = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        result.append(doc)
    return result

### 2. Data Aggregation
@app.get("/analytics/supplier-stats")
async def get_supplier_analytics():
    """
    Aggregates data to find the average distance covered 
    and total trip count for each Supplier.
    """
    pipeline = [
        {
            "$group": {
                "_id": "$supplierID",
                "avg_distance": {"$avg": "$TRANSPORTATION_DISTANCE_IN_KM"},
                "total_trips": {"$sum": 1},
                "delayed_count": {
                    "$sum": {"$cond": [{"$eq": ["$status", "Delayed"]}, 1, 0]}
                }
            }
        },
        {"$sort": {"avg_distance": -1}}
    ]
    
    results = await trips.aggregate(pipeline).to_list(length=100)
    return results

### 3. Delay Intensity Analysis (By Route)
@app.get("/analytics/delay-intensity-analysis-stats")
async def get_delay_intensity_analysis():
    """
    Identifies routes that act as bottlenecks by calculating 
    the percentage of delayed trips per route.
    """
    pipeline = [
        {
            # Group by the specific Route (Origin -> Destination)
            "$group": {
                "_id": {
                    "origin": "$Origin_Location",
                    "destination": "$Destination_Location"
                },
                "total_trips": {"$sum": 1},
                "delayed_trips": {
                    "$sum": {"$cond": [{"$eq": ["$status", "Delayed"]}, 1, 0]}
                },
                "avg_km": {"$avg": "$TRANSPORTATION_DISTANCE_IN_KM"}
            }
        },
        {
            # Calculate the intensity (percentage) of delays
            "$project": {
                "_id": 0,
                "route": "$_id",
                "total_trips": 1,
                "delayed_trips": 1,
                "delay_intensity_percentage": {
                    "$cond": [
                        {"$eq": ["$total_trips", 0]}, 
                        0, 
                        {"$multiply": [{"$divide": ["$delayed_trips", "$total_trips"]}, 100]}
                    ]
                },
                "average_distance": "$avg_km"
            }
        },
        # Sort by the most problematic routes first
        {"$sort": {"delay_intensity_percentage": -1}}
    ]
    
    results = await trips.aggregate(pipeline).to_list(length=100)
    return results

### 4. Fleet Efficiency (KM per Trip Type)
@app.get("/analytics/fleet-efficiency")
async def get_fleet_efficiency():
    """
    Analyzes distance distribution between 'Market' and 'Owned' vehicles.
    Helps determine if long-haul trips are being assigned efficiently.
    """
    pipeline = [
        {
            # Group by MarketType (e.g., Market vs Owned)
            "$group": {
                "_id": "$MarketType",
                "avg_distance": {"$avg": "$TRANSPORTATION_DISTANCE_IN_KM"},
                "max_distance": {"$max": "$TRANSPORTATION_DISTANCE_IN_KM"},
                "min_distance": {"$min": "$TRANSPORTATION_DISTANCE_IN_KM"},
                "total_trips": {"$sum": 1}
            }
        },
        {
            # Format the output for better readability
            "$project": {
                "market_type": { "$ifNull": ["$_id", "Unknown"] },
                "avg_distance_km": { "$round": ["$avg_distance", 2] },
                "trip_count": "$total_trips",
                "distance_range": {
                    "min": "$min_distance",
                    "max": "$max_distance"
                },
                "_id": 0
            }
        },
        {"$sort": {"avg_distance_km": -1}}
    ]
    
    results = await trips.aggregate(pipeline).to_list(length=10)
    return results