import pyodbc


def appendtomain_extent(admin_level, analysis_type, indicator_id, curs, conn, select_query_sql=None, value_to_sum=None):

    # take the indicator query and join to admin to get name of country
    print "admin level: {0} \nanalysis type: {1} \nindicator_id: {2}".format(admin_level, analysis_type, indicator_id)
    indicator_id_var = ', {} '.format(indicator_id)
    collection_table = 'tbl_collect_{}_values'.format(analysis_type)

    if admin_level == 'nat':
        subnat_var = ""
        select_qry_subnat_var = ""
        subnat_var_comma = ""
    else:
        subnat_var = 'adm1 '
        subnat_var_comma = ', adm1 '
        select_qry_subnat_var = ", {}.{}".format(collection_table, subnat_var)
    if analysis_type == 'extent':
        year_var = ""
    else:
        year_var = ', year '

    # delete existing select query
    try:
        conn.execute("DROP TABLE select_query")
    except pyodbc.ProgrammingError:
        pass

    # if I don't send a select query into this, then use this one below which just groups
    # by and sums, both for nat and subnat
    if not select_query_sql:
        print "no select query sent"

        select_query_sql = "SELECT iso, IIf([thresh]=0,1,[thresh]) AS thresh2, boundary_name, " \
                           "Sum({0}) AS [Value] {1} AS indicator_id {2} {3}" \
                           "FROM {4} " \
                           "GROUP BY iso, IIf([thresh]=0,1,[thresh]), boundary_name {1} {2} {3};".format(value_to_sum, indicator_id_var, subnat_var_comma, year_var, collection_table)

    # fill in select query with appropriate variables
    else:
        select_query_sql = select_query_sql.format(select_qry_subnat_var)

    print select_query_sql

    rows = curs.execute('CREATE PROC {0} AS {1}'.format('select_query', select_query_sql))

    rows.commit()

    query_join = "SELECT {0}.iso, thresh2, boundary_name, Value, indicator_id, adm0.NAME_ENGLISH {1} {2}" \
                 "FROM adm0 " \
                 "INNER JOIN {0} ON adm0.ISO = {0}.iso;".format('select_query', subnat_var_comma, year_var)

    # get rows of the select query
    result_rows = curs.execute(query_join)

    # this is what we want to insert into the main table
    insert_SQL = "INSERT INTO tbl_main (ISO, thresh, boundary, [Value], indicator_id, " \
                 "admin0_name, iso_and_subnat, sub_nat_id, year) " \
                 "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"


    for row in result_rows.fetchall():

        if admin_level == 'nat':
            iso_and_subnat = row[0]
            sub_nat_id = None

            if analysis_type == 'year':

                year = row[6]
            else:
                year = 0
        else:
            iso_and_subnat = str(row[0]) + str(row[6])
            sub_nat_id = row[6]

            if analysis_type == 'year':
                year = row[7]
            else:
                year = 0

        # we always get back the first 5 rows, only changes is tacked onto the end
        params = tuple(row[0:6]) + (iso_and_subnat, sub_nat_id, year)

        curs.execute(insert_SQL, params)

    conn.commit()



