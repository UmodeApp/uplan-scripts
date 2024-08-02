import os
import src.utils.colmeia_conection as colmeia_conection
from dotenv import load_dotenv
import csv
from datetime import datetime

load_dotenv()


def write_to_csv(file_name, data, mode='a'):
    with open(file_name, mode, newline='') as file:
        writer = csv.writer(file)
        writer.writerows(data)


def log_error(file_name, message):
    with open(file_name, 'a') as file:
        file.write(f"{datetime.now()}: {message}\n")


username = os.getenv("COLMEIA_USER")
password = os.getenv("COLMEIA_PASSWORD")
dsn = os.getenv("COLMEIA_URL_DB")


conn = colmeia_conection.connect_to_oracle(username, password, dsn)
if conn:
    today = datetime.now().strftime('%Y-%m-%d')
    offset = 1
    batch_size = 50

    while True:
        query = f"""
        SELECT * FROM UCOLMEIA.CR_UROCKET_ESTOQUE 
        ORDER BY CD_PRODUTO
        OFFSET :offset ROWS FETCH NEXT :batch_size ROWS ONLY
        """
        query = f"""SELECT * FROM UCOLMEIA.CR_UROCKET_LOJAS;"""
        query_params = {'offset': offset, 'batch_size': batch_size}
        query_params = {}

        try:
            data = colmeia_conection.execute_query(conn, query, query_params)
            if not data:
                break

            write_to_csv('estoque_data.csv', data)
            print(f"Batch iniciado em {offset} extraído com sucesso!")
            offset += batch_size
        except Exception as e:
            error_message = f"Erro ao buscar dados para o batch iniciado em {offset}: {e}"
            log_error('error_log.txt', error_message)
            print(error_message)
            break

    conn.close()
