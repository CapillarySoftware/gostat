USE gostat;

CREATE TABLE IF NOT EXISTS raw_stats (
   name    varchar,
   ts      timestamp,
   value   double,
   PRIMARY KEY (name, ts)
);