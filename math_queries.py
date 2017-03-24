import utilities
import insert_queries

#building this out to append more complicated queries to main tbale
database = r"C:\Users\samantha.gibbes\Documents\gis\hansen_2015\to_delete\hansen_2015_practice.accdb"
table_name = 'tbl_indicators_w_id'
curs, conn = utilities.connect_to_db(database)
print "connected to db"

# read in the table containing sql queries
results = utilities.put_data_into_dict(curs, 'tbl_indicator_queries')
for row in results:
    ind_id = row['indicator_id']
    if ind_id == 5:
        sql = row['sql']

admin_level = "subnat"
analysis_type = "extent"
indicator_id = 5
value_to_sum = "nat"

insert_queries.appendtomain_extent(admin_level, analysis_type, indicator_id, curs, conn, sql)
