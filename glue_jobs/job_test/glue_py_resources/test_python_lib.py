def get_two():
    return 2

def read_sql(path):
    with open(path, "r") as f:
        sql = f.readlines()
        sql = "".join(sql)
    return sql

from ast import literal_eval

def convert_to_type(row):
    for i,v in enumerate(row):
        try:
            row[i] = literal_eval(v)
        except:
            pass
    return row