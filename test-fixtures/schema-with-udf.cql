CREATE KEYSPACE bench WITH replication = {'class': 'NetworkTopologyStrategy', 'dc1': '3'};

CREATE TABLE bench.events (
  user_id uuid,
  event_ts timestamp,
  payload text,
  PRIMARY KEY (user_id, event_ts)
);

CREATE TABLE bench.profile (
  user_id uuid PRIMARY KEY,
  name text,
  email text
);

CREATE TABLE bench.users (
  user_id uuid PRIMARY KEY,
  email text
);

CREATE FUNCTION bench.normalize_email (e text)
  CALLED ON NULL INPUT
  RETURNS text
  LANGUAGE java
  AS $$ return e == null ? null : e.toLowerCase(); $$;
