CREATE KEYSPACE bench WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '3'};

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
