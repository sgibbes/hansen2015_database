import pyodbc

# path to db
db1 = r"C:\Users\samantha.gibbes\Documents\gis\hansen_2015\hansen_2015.accdb"

# create access connection
conn = pyodbc.connect(r'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={}'.format(db1))
curs = conn.cursor()

# write sql + execute
indicator_tbl = 'tbl_indicators_w_id'
tcd_list = [10, 15, 20, 25, 30, 50, 75]

sql_loss = 'INSERT INTO ' \
           'tbl_summed_values ( adm0, adm1, adm2, loss_yr, tcd, area_of_loss, count_of_loss, biomass ) ' \
           'SELECT ' \
           'Temp_loss_results_2000_lines.adm0, ' \
           'Temp_loss_results_2000_lines.adm1, ' \
           'Temp_loss_results_2000_lines.adm2, ' \
           'Temp_loss_results_2000_lines.year, ' \
           '{} AS tcd, ' \
           'Sum(Temp_loss_results_2000_lines.area) AS SumOfarea, ' \
           'Sum(Temp_loss_results_2000_lines.count) AS SumOfcount, ' \
           'Sum(Temp_loss_results_2000_lines.biomass) AS SumOfbiomass ' \
           'FROM ' \
           'Temp_loss_results_2000_lines ' \
           'WHERE (((Temp_loss_results_2000_lines.thresh)>{})) ' \
           'GROUP BY ' \
           'Temp_loss_results_2000_lines.adm0, ' \
           'Temp_loss_results_2000_lines.adm1, ' \
           'Temp_loss_results_2000_lines.adm2, ' \
           'Temp_loss_results_2000_lines.year, ' \
           '{};'

sql_make_extent_table = 'SELECT Treecover2000_first2000_lines.adm0, Treecover2000_first2000_lines.adm1, ' \
                        'Treecover2000_first2000_lines.adm2, Sum(Treecover2000_first2000_lines.extent) ' \
                        'AS SumOfextent, {0} AS Expr1 INTO tbl_temp ' \
                        'FROM Treecover2000_first2000_lines ' \
                        'WHERE (((Treecover2000_first2000_lines.tcd)>={1})) ' \
                        'GROUP BY Treecover2000_first2000_lines.adm0, ' \
                        'Treecover2000_first2000_lines.adm1, ' \
                        'Treecover2000_first2000_lines.adm2, {2};'

sql_make_biomass_extent_table = 'SELECT biomassextent_first2000_lines.adm0, biomassextent_first2000_lines.adm1, ' \
                                'biomassextent_first2000_lines.adm2, Sum(biomassextent_first2000_lines.biomass) ' \
                                'AS SumOfbiomass, {0} AS tcd INTO tbl_temp_biomass ' \
                                'FROM biomassextent_first2000_lines ' \
                                'WHERE (((biomassextent_first2000_lines.tcd)>={1})) ' \
                                'GROUP BY biomassextent_first2000_lines.adm0, biomassextent_first2000_lines.adm1, ' \
                                'biomassextent_first2000_lines.adm2, {2};'

sql_update_extent = 'UPDATE ' \
                    'tbl_temp ' \
                    'INNER JOIN ' \
                    'tbl_summed_values ' \
                    'ON ' \
                    '(tbl_temp.Expr1 = tbl_summed_values.tcd) ' \
                    'AND ' \
                    '(tbl_temp.adm2 = tbl_summed_values.adm2) ' \
                    'AND ' \
                    '(tbl_temp.adm1 = tbl_summed_values.adm1) ' \
                    'AND ' \
                    '(tbl_temp.adm0 = tbl_summed_values.adm0) ' \
                    'SET tbl_summed_values.extent2000 = [tbl_temp].[SumOfextent];'

sql_update_biomass_extent = 'UPDATE tbl_temp_biomass ' \
                            'INNER JOIN tbl_summed_values ON (tbl_temp_biomass.tcd = tbl_summed_values.tcd) ' \
                            'AND (tbl_temp_biomass.adm2 = tbl_summed_values.adm2) ' \
                            'AND (tbl_temp_biomass.adm1 = tbl_summed_values.adm1) ' \
                            'AND (tbl_temp_biomass.adm0 = tbl_summed_values.adm0) ' \
                            'SET tbl_summed_values.biomass_extent = [tbl_temp_biomass].[SumOfbiomass];'


def run_sql(tcd, qry):
    curs.execute(qry.format(tcd, tcd, tcd))

# run a group by and sum query from the raw data for each tcd thresh
# data from raw tables is appended to "tbl_summed_values"
for tcd in tcd_list:
    # append loss data to main table
    print tcd
    print sql_loss
    run_sql(tcd, sql_loss)

    # make temp table of extent grouped by and summed
    run_sql(tcd, sql_make_extent_table)

    # make temp table of biomass extent grouped by and summed
    run_sql(tcd, sql_make_biomass_extent_table)

    # update the main table with the temp table of extent
    run_sql(tcd, sql_update_extent)

    # update the main table with the temp table of biomass extent
    # run_sql(tcd, sql_update_biomass_extent)

    # delete the extent table
    curs.execute("DROP TABLE tbl_temp;")

    # delete the biomass extent table
    curs.execute("DROP TABLE tbl_temp_biomass")
    conn.commit()
