import pyodbc


def connect_to_db(my_db):
    # create access connection
    conn = pyodbc.connect(r'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={}'.format(my_db))
    curs = conn.cursor()

    return curs, conn


def put_data_into_dict(curs, table_name):
    # write sql + execute
    sql = 'SELECT * FROM {}'.format(table_name)
    rows = curs.execute(sql).fetchall() # don't have to do fetchall

    # set up empty result list
    results = []

    # grab column names AFTER we've executed our query
    columns = [column[0] for column in curs.description]

    # Iterate over all query response
    # each time creating a dictionary of column_name: column_value

    for row in rows:
        results.append(dict(zip(columns, row)))

    return results


def insert_query(sel_query):
    insert_into_tbl_main = """
    INSERT INTO tbl_main ( ISO, admin0_name, boundary, indicator_id, thresh, [Year], [Value],
    text_value, iso_and_subnat, sub_nat_id )
    SELECT {0}.adm0, adm0.NAME_ENGLISH, ? AS bound, ? AS ind_id, {0}.tcd, [loss_yr] AS lossyr, Sum({0}.Value) AS val,
    ? AS txt, replace([{0}]![adm0]+Str([{0}]![adm1]), ?, ?) AS iso_sub, {0}.adm1
    FROM adm0 INNER JOIN {0} ON (adm0.ISO = {0}.adm0) AND ({0}.adm0 = adm0.ISO) AND (adm0.ISO = {0}.adm0)
    GROUP BY {0}.adm0, adm0.NAME_ENGLISH, ?, ?, {0}.tcd, [loss_yr], ?, replace([{0}]![adm0]+Str([{0}]![adm1]),?,?), {0}.adm1;
    """.format(sel_query)

    return insert_into_tbl_main


def insert_qry_ind2_subnat(sel_query):
    insert_into_tbl_main_natl = """
    INSERT INTO tbl_main ( ISO, admin0_name, boundary, indicator_id, thresh, [Year], [Value], text_value,
    iso_and_subnat, sub_nat_id )
    SELECT {0}.adm0, adm0.NAME_ENGLISH, ? AS bound, ? AS ind_id, {0}.tcd, {0}.[loss_yr] AS lossyr, Sum({0}.Value) AS val,
    ? AS txt, {0}.adm0, ? AS Expr1
    FROM adm0 INNER JOIN {0} ON (adm0.ISO = {0}.adm0) AND (adm0.ISO = {0}.adm0) AND (adm0.ISO = {0}.adm0)
    GROUP BY {0}.adm0, adm0.NAME_ENGLISH, ?, ?, {0}.tcd, {0}.[loss_yr], ?, {0}.adm0, ?;
    """.format(sel_query)

    return insert_into_tbl_main_natl


def append_select_queries(cursor, conn, sel_qry_list):
    ind_to_append = [2]
    for sel_query in sel_qry_list:
        ind_id = int(sel_query.split("_")[1])

        if ind_id in ind_to_append:
            print "indicator {}".format(ind_id)

            ins_qry = insert_query(sel_query)
            # print ins_qry
            # params = ("admin", ind_id, "null", " ", "", "admin", ind_id, "null", " ", "")
            # cursor.execute(ins_qry, params)
            # print "executed query"

            ins_qry_nat = insert_qry_natl(sel_query)
            params2 = ("admin", ind_id, "null", "null", "admin", ind_id, "null", 'null')
            cursor.execute(ins_qry_nat, params2)
            print "executed query"

            conn.commit()

            del ins_qry
            del ins_qry_nat


def get_sel_query_list(cursor):

    sel_qry_list = []
    for rows in cursor.tables():

        table_name = rows.table_name

        if "ind_" in table_name:

            sel_qry_list.append(table_name)

    return sel_qry_list


def build_select_queries(curs, table_name):
    # put the data from table into a dictionary so it can be easily read
    data_dict = put_data_into_dict(curs, table_name)

    for indicator_dict in data_dict:

        source = indicator_dict['source']
        if source == 'internal':
            # if the indicator is internal, we need to create a query by reading the sql statement column
            sql_statement = indicator_dict['sql_statement']

            if sql_statement:
                # if there is a sql statement in this column, create a query for it

                # create a name for the query
                qry_name_st = indicator_dict['qry_name']
                indic_id = int(indicator_dict['indicator_id'])

                print "indicator id: {}, sql statement: {}".format(indic_id, sql_statement)
                qry_name = 'ind_{}_{}'.format(indic_id, qry_name_st)
                print qry_name

                # create a saved query after reading in sql statement column from tbl_indicators w id
                rows = curs.execute('CREATE PROC {} AS {}'.format(qry_name, sql_statement))
                rows.commit()

# connected to the access database
database = r"C:\Users\samantha.gibbes\Documents\gis\hansen_2015\hansen_2015.accdb"
table_name = 'tbl_indicators_w_id'
curs, conn = connect_to_db(database)
print "connected to db"

# build_select_queries(curs, table_name)
#
sel_qry_list = get_sel_query_list(curs)

# append indicator queries to "tbl_main"
append_select_queries(curs, conn, sel_qry_list)