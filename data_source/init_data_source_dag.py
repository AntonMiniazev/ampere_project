from db.db_io import exec_sql

test_sql = """ SELECT TOP 1 * FROM Source.core.orders"""

test_output = exec_sql(test_sql)
print(test_output)
