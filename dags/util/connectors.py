import psycopg2
import psycopg2.extras as extras

import datetime


class BackendDatabase:
    def __init__(self, host, port,dbname,user,password):
        """
        Initialize Validation Data

        validation = validator.Validator(dataframe, dataframe_validator)

        Parameters
        ----------
        host

        Returns
        ----------
        new Validator class

        Atributes
        ----------

        """

        self.conn = psycopg2.connect(
            host=host,
            port=port,
            dbname=dbname,
            user=user,
            password=password
            # host="107.0.0.1",
            # port=5432,
            # dbname="airflow",
            # user="airflow",
            # password="airflow"
        )

    def generate_create_table(self, df, table_name):
        create_table_script = f"CREATE TABLE {table_name} (\n"
        columns = df.columns

        for col in columns:
            dtype = df[col].dtype
            
            if dtype == 'int64':
                column_type = 'INT'
            elif dtype == 'float64':
                column_type = 'FLOAT'
            elif dtype == 'bool':
                column_type = 'BOOLEAN'
            elif dtype == 'object':
                column_type = 'TEXT'
            else:
                column_type = 'TEXT'

            create_table_script += f"    {col} {column_type},\n"

        create_table_script = create_table_script.rstrip(',\n') + "\n);"

        return create_table_script
    
    def execute_create_table(self, script, table_name):
        try:
            # Conexão com o banco de dados
            cursor = check_conn_is_on(self.conn)

                    # Verifica se a tabela existe
            cursor.execute(f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')")
            exists = cursor.fetchone()[0]
            if not exists:
                # Executa o script de criação da tabela
                cursor.execute(script)

                # Confirma as alterações no banco de dados
                self.conn.commit()

                # Fecha a conexão
                cursor.close()

                print("Tabela criada com sucesso!")
        except (Exception, psycopg2.Error) as error:
            print("Erro ao criar a tabela:", error)
            self.conn.rollback()
            cursor.close()


    def execute_values(self,df, table, cols=None):
        tuples = [tuple(x) for x in df.to_numpy()]

        # Define as colunas a serem inseridas
        if cols is None:
            cols = ','.join(list(df.columns))
        else:
            cols = ','.join(cols)

        # SQL query to execute
        query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
        cursor = check_conn_is_on(self.conn)
        try:
            extras.execute_values(cursor, query, tuples)
            self.conn.commit()
        except (Exception, psycopg2.DatabaseError) as error:
            message_error = f"Erro ao inserir Modelo de dados no PostgreSQL."
            print(message_error + '\n' + str(error))

            self.conn.rollback()
            cursor.close()

            return 1

        print("the dataframe is inserted")
        cursor.close()

   
    def execute_delete_query(self,table_name, where_clause=None):
        try:
            cursor = check_conn_is_on(self.conn)

            # Monta a consulta SQL com base na cláusula WHERE
            if where_clause is None:
                query = f"DELETE FROM {table_name};"
            else:
                query = f"DELETE FROM {table_name} WHERE {where_clause}"

            cursor.execute(query)
            self.conn.commit()
            text = f'{table_name} deletada com sucesso!'

        except (Exception, psycopg2.Error) as error:
            text = f"Erro ao executar a consulta: {error}"

        print(text)
        cursor.close()
        return text

def check_conn_is_on(conn):

    if conn is not None and conn.closed == 0:
        cursor = conn.cursor()
        return cursor
    else:
        cursor = conn.cursor()
        return cursor
