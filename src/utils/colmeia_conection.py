import cx_Oracle


def connect_to_oracle(username: str, password: str, dsn: str):
    try:
        connection = cx_Oracle.connect(
            user=username, password=password, dsn=dsn)
        print("Conexão bem-sucedida ao Oracle!")
        return connection
    except cx_Oracle.DatabaseError as e:
        print(f"Erro ao conectar ao Oracle: {e}")
        return None


def insert_record(connection, table_name: str, data: dict):
    placeholders = ", ".join([f":{k}" for k in data.keys()])
    columns = ", ".join(data.keys())
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

    try:
        cursor = connection.cursor()
        cursor.execute(sql, data)
        connection.commit()
        print("Registro inserido com sucesso!")
    except cx_Oracle.DatabaseError as e:
        print(f"Erro ao inserir o registro: {e}")
    finally:
        cursor.close()


def find_records(connection, table_name: str, query: str, query_params: dict = {}):
    sql = f"SELECT * FROM {table_name} WHERE {query}"

    try:
        cursor = connection.cursor()
        cursor.execute(sql, query_params)
        rows = cursor.fetchall()
        return rows
    except cx_Oracle.DatabaseError as e:
        print(f"Erro ao buscar registros: {e}")
        return []
    finally:
        cursor.close()


def update_records(connection, table_name: str, update_data: dict, condition: str, condition_params: dict):
    set_clause = ", ".join([f"{k} = :{k}" for k in update_data.keys()])
    sql = f"UPDATE {table_name} SET {set_clause} WHERE {condition}"
    params = {**update_data, **condition_params}

    try:
        cursor = connection.cursor()
        cursor.execute(sql, params)
        connection.commit()
        print(f"{cursor.rowcount} registro(s) atualizado(s) com sucesso!")
    except cx_Oracle.DatabaseError as e:
        print(f"Erro ao atualizar registros: {e}")
    finally:
        cursor.close()


def execute_query(connection, query: str, query_params: dict = {}):
    try:
        cursor = connection.cursor()
        cursor.execute(query, query_params)
        rows = cursor.fetchall()
        return rows
    except cx_Oracle.DatabaseError as e:
        print(f"Erro ao executar a query: {e}")
        return []
    finally:
        cursor.close()


def find_last_10_sales(connection):
    sql = """
    SELECT * FROM (
        SELECT CD_PRODUTO FROM UCOLMEIA.CR_UROCKET_PRODUTOS 
    )
    """

    try:
        cursor = connection.cursor()
        cursor.execute(sql)
        rows = cursor.fetchall()
        return rows
    except cx_Oracle.DatabaseError as e:
        print(f"Erro ao buscar registros: {e}")
        return []
    finally:
        cursor.close()
