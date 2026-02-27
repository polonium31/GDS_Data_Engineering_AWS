from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# 1. Connection Details (from your screenshot)
auth_provider = PlainTextAuthProvider(
    username='', 
    password=''
)

# Contact point from your screenshot
nodes = ['node-0.aws-us-east-1.3d14f0ed92b243eb1fc6.clusters.scylla.cloud']

print("Connecting to ScyllaDB Cloud...")
cluster = Cluster(nodes, auth_provider=auth_provider)
session = cluster.connect()

# 2. Create the Keyspace 
# Note: 'AWS_US_EAST_1' matches the DC name in your screenshot
print("Creating keyspace 'advertising'...")
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS advertising 
    WITH replication = {'class': 'NetworkTopologyStrategy', 'AWS_US_EAST_1': 3};
""")

# 3. Create the Table
# ad_id is the Primary Key to enable the 'Upsert' logic in your Spark job
print("Creating table 'ads_metrics'...")
session.execute("""
    CREATE TABLE IF NOT EXISTS advertising.ads_metrics (
        ad_id text PRIMARY KEY,
        total_clicks int,
        total_views int,
        avg_cost_per_view double
    );
""")

print("✅ Setup complete! You can now run your Spark job.")
cluster.shutdown()