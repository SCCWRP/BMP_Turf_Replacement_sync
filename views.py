from functions import exception_handler

# We need this function because there are tables that this view depends on, and those tables get dropped with CASCADE and recreated
# The fact that those tables are dropped with CASCADE means that this view also gets dropped every time the sync runs
# Therefore we must recreate the view at the end of the sync process
@exception_handler
def create_views(eng):
    print("vw_watervolume")
    eng.execute(
        """
            DROP MATERIALIZED VIEW IF EXISTS vw_watervolume;
            CREATE MATERIALIZED VIEW vw_watervolume AS (
                SELECT 
                    tbl_watervolume.origin_filename,
                    tbl_sensorid.sitename,
                    tbl_sensorid.sitelocation,
                    tbl_sensorid.sensorgroup,
                    tbl_sensorid.sensorsequencenumber,
                    tbl_watervolume.sensor,
                    tbl_watervolume."timestamp",
                    tbl_watervolume.battv,
                    COALESCE(tbl_watervolume.wvc_final , tbl_watervolume.wvc_prelim_calc) AS wvc_final,
                    tbl_watervolume.wvc_calc,
                    tbl_watervolume.wvc_prelim_calc,
                    tbl_watervolume.wvc_raw,
                    tbl_watervolume.wvcunit,
                    tbl_watervolume.ka_final,
                    tbl_watervolume.ka_calc,
                    tbl_watervolume.ka_raw,
                    tbl_watervolume.kaunit,
                    tbl_watervolume.ec,
                    tbl_watervolume.ecunit,
                    tbl_watervolume.pa,
                    tbl_watervolume.paunit,
                    tbl_watervolume.calculated,
                    tbl_watervolume.highka,
                    tbl_watervolume.lowka,
                    tbl_watervolume.kalimit,
                    tbl_watervolume.kalimit80pct
                FROM (tbl_watervolume
                    LEFT JOIN tbl_sensorid ON (((tbl_watervolume.sensor)::text = tbl_sensorid.sensor)))
                ORDER BY tbl_watervolume."timestamp", tbl_sensorid.sitename, tbl_sensorid.sensorsequencenumber, tbl_sensorid.sensorgroup, tbl_sensorid.sensor
            )
        """
    )
    # WHERE (tbl_watervolume.wv IS NOT NULL) used to be in there, but I removed it on 10/7/2022 so they can see for themselves why it came up as a null value

    print("Done creating vw_watervolume")

    print("vw_rainevent")
    eng.execute(
        """
        CREATE OR REPLACE VIEW vw_rainevent AS (
            WITH raindata AS (
                SELECT DISTINCT tbl_rainevent.region,
                    tbl_rainevent.rainstart,
                    tbl_rainevent.rainend,
                    tbl_rainevent.totaldepth
                FROM tbl_rainevent
            )
            SELECT lu_nearestraingauge.site AS sitename,
                raindata.rainstart,
                raindata.rainend,
                raindata.totaldepth
            FROM raindata
                JOIN lu_nearestraingauge ON raindata.region = lu_nearestraingauge.raingauge
        )
        """
    )
    print("Done creating vw_rainevent")
   
    print("Update tbl_watervolume_final")
    eng.execute(
        """
        INSERT INTO tbl_watervolume_final 
        (SELECT * FROM vw_watervolume) 
        ON CONFLICT ON CONSTRAINT tbl_watervolume_final_pkey 
        DO UPDATE 
        SET 
            origin_filename = EXCLUDED.origin_filename,
            sitename = EXCLUDED.sitename,
            sitelocation = EXCLUDED.sitelocation,
            sensorgroup = EXCLUDED.sensorgroup,
            sensorsequencenumber = EXCLUDED.sensorsequencenumber,
            wvc_final = EXCLUDED.wvc_final,
            wvc_calc = EXCLUDED.wvc_calc,
            wvc_prelim_calc = EXCLUDED.wvc_prelim_calc,
            wvc_raw = EXCLUDED.wvc_raw,
            ka_final = EXCLUDED.ka_final,
            ka_calc = EXCLUDED.ka_calc,
            ka_raw = EXCLUDED.ka_raw,
            ec = EXCLUDED.ec,
            pa = EXCLUDED.pa,
            calculated = EXCLUDED.calculated,
            highka = EXCLUDED.highka,
            lowka = EXCLUDED.lowka,
            kalimit = EXCLUDED.kalimit,
            kalimit80pct = EXCLUDED.kalimit80pct,
            battv = EXCLUDED.battv
        """
    )
    print("Done updating tbl_watervolume_final")

    
    return ['The materialized views vw_watervolume and vw_rainevent were recreated successfully']