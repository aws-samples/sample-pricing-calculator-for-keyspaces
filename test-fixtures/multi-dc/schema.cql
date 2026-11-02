CREATE KEYSPACE bench WITH replication = {'class': 'NetworkTopologyStrategy', 'us-east-1': '3', 'dc-eu': '3'};

CREATE TABLE bench.events (
  user_id uuid,
  event_ts timestamp,
  payload text,
  PRIMARY KEY (user_id, event_ts)
);

CREATE TABLE bench.profile (
  user_id uuid PRIMARY KEY,
  name text
);
