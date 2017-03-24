# hansen2015_database
When adding new data for indicators 1, 3 or 4, use add_to_tbl_summed_values.py
When adding more complex queries, use math_queries.py. This pulls from a table with queries hard coded and variables to fill in nat/subnat.
Both of these scenaries send queries to insert_queries.py. This either constructs a new query based on that pulled from the table "tbl_indicator_queries" or uses the hard coded query that is in the database

*Indicator 1: create_ind1_ind3_queries
Indicator 2: create_ind1_ind3_queries
*Indicator 3: create_ind1_ind3_queries

* must do these before ind2

Indicator 4: add_to_tbl_summed_values.py
Indicator 5: math_queries.py
