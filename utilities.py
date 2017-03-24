import pyodbc


def connect_to_db(my_db):
    # create access connection
    conn = pyodbc.connect(r'DRIVER={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={}'.format(my_db))
    curs = conn.cursor()

    return curs, conn


def create_queries(curs, results):
    # create sql select queries
    for row in results:
        ind_id = row['indicator_id']
        admin_level = row['admin_level']
        sql_statement = row['sql']
        sql_name = "qry_ind{0}_{1}_test".format(ind_id, admin_level)

        rows = curs.execute('CREATE PROC {} AS {}'.format(sql_name, sql_statement))
        rows.commit()


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