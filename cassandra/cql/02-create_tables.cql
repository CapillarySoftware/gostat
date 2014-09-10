USE gostat;

CREATE TABLE IF NOT EXISTS raw_stats (
   name    varchar,
   ts      timestamp,
   value   float,
   PRIMARY KEY (name, ts)
);