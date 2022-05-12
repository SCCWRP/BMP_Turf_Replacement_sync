from functions import exception_handler

# We need this function because there are tables that this view depends on, and those tables get dropped with CASCADE and recreated
# The fact that those tables are dropped with CASCADE means that this view also gets dropped every time the sync runs
# Therefore we must recreate the view at the end of the sync process
@exception_handler
def create_views(eng):
    print("vw_watervolume")
    eng.execute(
        """
            CREATE MATERIALIZED VIEW vw_watervolume AS (
                SELECT 
                    tbl_watervolume.origin_filename,
                    tbl_sensorid.sitename,
                    tbl_sensorid.sitelocation,
                    tbl_sensorid.sensorgroup,
                    tbl_sensorid.sensorsequencenumber,
                    tbl_watervolume.sensor,
                    tbl_watervolume."timestamp",
                    tbl_watervolume.result,
                    tbl_watervolume.unit
                FROM (tbl_watervolume
                    LEFT JOIN tbl_sensorid ON (((tbl_watervolume.sensor)::text = tbl_sensorid.sensor)))
                WHERE (tbl_watervolume.result IS NOT NULL)
                ORDER BY tbl_watervolume."timestamp", tbl_sensorid.sitename, tbl_sensorid.sensorsequencenumber, tbl_sensorid.sensorgroup, tbl_sensorid.sensor
            )
        """
    )
    return ['The materialized view vw_watervolume was recreated successfully']