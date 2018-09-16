create table bike_locations
as
select *
from dblink('host=bikehero-dev.crvkivsnqb9c.us-east-1.rds.amazonaws.com
	     user=mark
	     password=KYp9hbyVmx6yH43
	     dbname=bikefinder',
	     'select * from bike_locations') as linktable(
				  location_id bigint,
				  location text,
				  provider text,
				  bike_id text,
				  created timestamp,
				  "raw" jsonb
				)