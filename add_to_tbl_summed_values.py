import utilities
import insert_queries

# connected to the access database
database = r"C:\Users\samantha.gibbes\Documents\gis\hansen_2015\to_delete\hansen_2015_practice.accdb"
table_name = 'tbl_indicators_w_id'
curs, conn = utilities.connect_to_db(database)
print "connected to db"

# ind 1, 3, 4
# admin_level = ['subnat', 'nat']
# analysis_type = ['extent', 'year']  # extent or year
# indicator_id = [1, 3, 4]
# value_to_sum = ['biomassextent_mg', 'extent_ha', 'area_ha']

indicator_dict = {3: {'analysis_type': 'extent', 'value_to_sum': 'extent_ha'},
        1: {'analysis_type': 'year', 'value_to_sum': 'area_ha'},
        4: {'analysis_type': 'extent', 'value_to_sum': 'biomassextent_mg'}}

indicator_id = 4
admin_level = 'subnat'
analysis_type = indicator_dict[indicator_id]['analysis_type']
value_to_sum = indicator_dict[indicator_id]['value_to_sum']

# insert queries 1, 3, 4 (simple group by/sum queries) into tbl_main
insert_queries.appendtomain_extent(admin_level, analysis_type, indicator_id, curs, conn, None, value_to_sum)



