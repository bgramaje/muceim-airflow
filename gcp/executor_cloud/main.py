import os
import sys
import base64
import zlib
from utils import get_ducklake_connection


def main():
    """Función principal del Cloud Run Job para ejecutar consultas SQL."""
    try:
        sql_query = os.environ.get("SQL_QUERY")

        if not sql_query:
            print("ERROR: Missing required environment variable: SQL_QUERY")
            sys.exit(1)

        print(f"[CLOUD_RUN_JOB] Executing SQL query...")

        # DuckDB configuration optimized for 32 GiB RAM and 8 CPUs
        config = {
            'memory_limit': '28GB',
            'threads': 8,
            'worker_threads': 8,
            'max_temp_directory_size': '200GiB'
        }
        con = get_ducklake_connection(duckdb_config=config)

        # Ejecutar la query SQL
        try:
            print(f"[CLOUD_RUN_JOB] Executing SQL query...")
            result = con.execute(sql_query)
            
            # Intentar obtener el DataFrame del resultado
            df_result = None
            try:
                df_result = result.fetchdf()
                print(f"[CLOUD_RUN_JOB] Query executed successfully, captured {len(df_result)} rows")
            except Exception:
                # Si no se puede obtener DataFrame (CREATE TABLE, etc.), está bien
                print(f"[CLOUD_RUN_JOB] SQL query executed successfully")
            
        except Exception as query_error:
            error_msg = f"Error executing SQL query: {str(query_error)}"
            print(f"[CLOUD_RUN_JOB] {error_msg}")
            raise

        # Ejecutar función de postprocesamiento si existe
        post_process_code = os.environ.get("POST_PROCESS_CODE")
        post_process_func_name = os.environ.get("POST_PROCESS_FUNC_NAME")
        
        if post_process_code and post_process_func_name:
            try:
                print(f"[CLOUD_RUN_JOB] Executing post-processing function: {post_process_func_name}")
                
                # Descomprimir y decodificar el código
                decoded = base64.b64decode(post_process_code.encode('utf-8'))
                decompressed = zlib.decompress(decoded).decode('utf-8')
                
                # Crear un namespace para ejecutar el código
                # El código serializado ya incluye todos los imports necesarios dentro de la función
                namespace = {
                    'con': con,
                    'result_dict': {
                        'status': 'success',
                        'message': 'SQL query executed successfully',
                        'execution_name': 'cloud_run',
                    },
                    '__builtins__': __builtins__,
                }
                
                # Ejecutar el código para definir la función
                # El código ya incluye los imports (numpy, pandas, sklearn, etc.) dentro de la función
                exec(decompressed, namespace)
                
                # Obtener la función y ejecutarla con el DataFrame como primer parámetro
                post_process_func = namespace.get(post_process_func_name)
                if post_process_func:
                    post_result = post_process_func(df_result, con, namespace['result_dict'])
                    
                    if post_result and isinstance(post_result, dict):
                        # Imprimir el resultado en un formato parseable para recuperarlo desde los logs
                        import json
                        result_json = json.dumps(post_result, default=str)
                        print(f"POST_PROCESS_RESULT_JSON:{result_json}")
                    
                    print(f"[CLOUD_RUN_JOB] Post-processing completed")
                else:
                    print(f"[WARNING] Post-processing function '{post_process_func_name}' not found in code")
                    
            except Exception as post_error:
                error_msg = f"Error in post-processing function: {str(post_error)}"
                print(f"[CLOUD_RUN_JOB] {error_msg}")
                import traceback
                traceback.print_exc()
                # No fallamos el job si el post-procesamiento falla, solo logueamos

        con.close()
        print("[CLOUD_RUN_JOB] Job completed successfully")
        sys.exit(0)

    except Exception as e:
        error_msg = f"Error processing job: {str(e)}"
        print(f"[CLOUD_RUN_JOB] {error_msg}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

