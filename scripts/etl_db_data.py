def extract_fn(conn_obj):
    select_query = "select * from tbl_source_audit;"
    # for sqlalchemy
    # df = pd.read_sql_query(select_query,conn_obj)
    # for pg hook
    df = conn_obj.get_pandas_df(select_query)
    print(df.head())
    return df 

def trasform_fn(df):
    df['dt_updated'] = dt.now()
    return df 

def load_fn(df,conn_obj):
    # for sql alchemy
    # df.to_sql("st_source_audit",conn_obj,if_exists="append",index=False)
    # conn_obj.execute("CALL sp_update_source_audit();")
    # for pg hook
    col = list(df.schema)
    rows = list(df.itertuples(index=False,name=None))
    conn_obj.insert_rows(table='st_source_audit',rows=rows,target_fields=col)
    
    cursor = conn_obj.get_conn().cursor() 
    cursor.execute("call sp_update_source_audit();")
    return 1


def etl():
    # conn obj with sqlalchemy
    # conn_obj = get_db_conn()
    # con obj with pg hook
    conn_obj = get_pg_hook_conn()
    e_df = extract_fn(conn_obj)
    t_df = transform_fn(e_df)
    l_rtn_obj = load_fn(t_df,conn_obj)