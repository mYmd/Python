# Copyright (c) 2022 mmYYmmdd
import pyodbc


def partial_gen(it, counter: int):
    for x in it:
        yield x
        counter -= 1
        if counter<=0:
            break


class idcount:
    def __init__(self, init=0):
        self.counter=init
    def __call__(self, x):
        self.counter+=1
        return x


def insert_expr_(schema, tablename, columnnames, insert_unit):
    u = f"({','.join(['?']*len(columnnames))})"
    return f'INSERT INTO {schema}.{tablename} ({",".join(columnnames)}) VALUES {",".join([u] * insert_unit)};'

#https://learn.microsoft.com/ja-jp/azure/azure-sql/performance-improve-use-batching?view=azuresql#multiple-row-parameterized-insert-statements

def fast_insert(con, schema, tablename, values, commit_unit):
    attr = con.cursor().columns(schema=schema, table=tablename).fetchall()
    columnnames = tuple(x[3] for x in attr)
    insert_unit = int(2099/len(columnnames))
    it = iter(values)
    total = 0
    while True:
        row_count = 0
        part = ()
        buf = []
        while row_count < commit_unit:
            ref = idcount()
            part = tuple(x for rec in partial_gen(it, insert_unit) for x in ref(rec) )
            row_count += ref.counter
            total += ref.counter
            if ref.counter < insert_unit:
                break
            buf.append(part)
            part = ()
        if len(part)==0 and buf:
            expr = insert_expr_(schema, tablename, columnnames, insert_unit)
            con.cursor().executemany(expr, buf)
            con.commit()
        elif part:
            expr = insert_expr_(schema, tablename, columnnames, ref.counter)
            con.cursor().execute(expr, part)
            con.commit()
            break
        if ref.counter == 0:
            break
    return total

'''
def main():
    #m = [(x, x*10, 'a') for x in range(0, 83880)]
    m = [(x+1, x*10+1, 'a') for x in range(0, 44444)]
    expr = 'Driver=***'
    con = pyodbc.connect(expr)
    con.execute('TRUNCATE TABLE dbo.Table01;')
    con.commit()
    total = fast_insert(con, 'dbo', 'Table01', iter(m), 16384)
    print(total)


if __name__ == '__main__':
    main()
'''

