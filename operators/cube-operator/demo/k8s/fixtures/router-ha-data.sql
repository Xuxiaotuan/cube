-- Deterministic small fixture for the destructive Cube API failover E2E.
CREATE SCHEMA IF NOT EXISTS __SCHEMA__;
CREATE TABLE __SCHEMA__.__TABLE__ (id bigint, amount bigint, region text, payload text);
INSERT INTO __SCHEMA__.__TABLE__ (id, amount, region, payload) VALUES
(1, 10, 'north', 'router-ha-north-01'),
(2, 20, 'north', 'router-ha-north-02'),
(3, 30, 'north', 'router-ha-north-03'),
(4, 40, 'south', 'router-ha-south-04'),
(5, 50, 'south', 'router-ha-south-05'),
(6, 60, 'south', 'router-ha-south-06'),
(7, 70, 'east', 'router-ha-east-07'),
(8, 80, 'east', 'router-ha-east-08'),
(9, 90, 'east', 'router-ha-east-09'),
(10, 100, 'west', 'router-ha-west-10'),
(11, 110, 'west', 'router-ha-west-11'),
(12, 120, 'west', 'router-ha-west-12');
