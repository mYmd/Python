# Copyright (c) 2020 mmYYmmdd
import pyodbc

def drivers_(dbsys, drivers = [], d_dict = {}):
    if not drivers:
        drivers += pyodbc.drivers()
        drivers_o = sorted([x for x in drivers if 'ORACLE' in x.upper()], key = len)
        drivers_s = sorted([x for x in drivers if 'SQL SERVER' in x.upper()], key = len)
        drivers_e = sorted([x for x in drivers if 'EXCEL' in x.upper()], key = len)
        drivers_a = sorted([x for x in drivers if 'MICROSOFT ACCESS DRIVER' in x.upper()], key = len)
        d_dict['O'] = drivers_o[-1] if drivers_o else ''
        d_dict['S'] = drivers_s[-1] if drivers_s else ''
        d_dict['E'] = drivers_e[-1] if drivers_e else ''
        d_dict['A'] = drivers_a[-1] if drivers_a else ''
    return d_dict.get(dbsys, '')

if drivers_(''):
    pass

def oracle_expr(dbq : str, uid : str, pwd : str, readonly : bool = True) -> str:
    driver = drivers_('O')
    return f'DBA={"R" if readonly else "W"}; Driver={{{driver}}}; DBQ={dbq}; UID={uid}; PWD={pwd};' if driver else ''

def sqlserver_expr(server : str = r'localhost\SQLEXPRESS', database : str ='sampleDB01') -> str:
    driver = drivers_('S')
    return f'Driver={{{driver}}}; Trusted_Connection=YES; Server={server}; DATABASE={database};' if driver else ''

def excel_expr(book_name : str, readonly : bool = True) -> str:
    driver = drivers_('E')
    return f'DBA={"R" if readonly else "W"}; Driver={{{driver}}}; DBQ={book_name};' if driver else ''

def access_expr(db_name : str, readonly : bool = True) -> str:
    driver = drivers_('A')
    return f'DBA={"R" if readonly else "W"}; Driver={{{driver}}}; DBQ={db_name};' if driver else ''

if __name__ == '__main__':
    print(f'oracle_expr => {oracle_expr("alps", "scott", "123")}')
    print(f'sqlserver_expr => {sqlserver_expr()}')
    print(f'excel_expr => {excel_expr("Book1")}')
    print(f'access_expr => {access_expr("sampleDB01")}')
#importlib.reload(module)
