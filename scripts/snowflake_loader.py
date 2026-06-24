import json
import logging
import pandas as pd
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
import snowflake.connector
from datetime import datetime

logger = logging.getLogger(__name__)

def load_to_snowflake(**context):
    bucket_name = "predictit-raw"
    
    # 1. Obtener el archivo desde MinIO (usando XCom o el más reciente)
    ti = context['task_instance']
    s3_path = ti.xcom_pull(task_ids='extract_predictit_to_minio', key='s3_path')
    
    s3_hook = S3Hook(aws_conn_id='minio')
    if s3_path:
        key = s3_path.replace(f"s3://{bucket_name}/", "")
        file_content = s3_hook.read_key(key=key, bucket_name=bucket_name)
    else:
        keys = s3_hook.list_keys(bucket_name=bucket_name, prefix="raw/")
        if not keys:
            raise ValueError("No se encontraron archivos en MinIO")
        latest_key = sorted(keys)[-1]
        file_content = s3_hook.read_key(key=latest_key, bucket_name=bucket_name)
    
    # 2. Parsear JSON
    data = json.loads(file_content)
    markets = data.get('markets', [])
    logger.info(f"Procesando {len(markets)} mercados")
    
    if not markets:
        logger.warning("No hay datos para insertar")
        return
    
    # 3. Preparar los datos para inserción (con toda la información anidada)
    rows = []
    for market in markets:
        # Datos del mercado (nivel principal)
        market_id = market.get('id')
        market_name = market.get('name')
        market_short_name = market.get('shortName')
        market_url = market.get('url')
        market_timestamp = market.get('timeStamp')
        market_status = market.get('status')
        market_last_trade = market.get('lastTradePrice')
        market_best_buy_yes = market.get('bestBuyYesCost')
        market_best_sell_yes = market.get('bestSellYesCost')
        market_best_buy_no = market.get('bestBuyNoCost')
        market_best_sell_no = market.get('bestSellNoCost')
        market_last_close = market.get('lastClosePrice')
        
        # Extraer los contratos (predicciones) de cada mercado
        contracts = market.get('contracts', [])
        
        if not contracts:
            # Si no tiene contratos, insertamos una fila con NULL en las columnas de contrato
            rows.append((
                market_id, market_name, market_short_name, market_url, 
                market_timestamp, market_status, market_last_trade,
                market_best_buy_yes, market_best_sell_yes, market_best_buy_no,
                market_best_sell_no, market_last_close,
                None, None, None, None, None, None, None, None, None, None
            ))
        else:
            for contract in contracts:
                rows.append((
                    market_id, market_name, market_short_name, market_url,
                    market_timestamp, market_status, market_last_trade,
                    market_best_buy_yes, market_best_sell_yes, market_best_buy_no,
                    market_best_sell_no, market_last_close,
                    contract.get('id'),
                    contract.get('name'),
                    contract.get('shortName'),
                    contract.get('lastTradePrice'),
                    contract.get('bestBuyYesCost'),
                    contract.get('bestSellYesCost'),
                    contract.get('bestBuyNoCost'),
                    contract.get('bestSellNoCost'),
                    contract.get('lastClosePrice'),
                    contract.get('displayOrder')
                ))
    
    logger.info(f"Total de filas a insertar: {len(rows)}")
    if rows:
        logger.info(f"Primera fila de ejemplo: {rows[0]}")
    
    # 4. Conectar a Snowflake
    conn = BaseHook.get_connection('snowflake_default')
    extra = conn.extra_dejson
    
    ctx = snowflake.connector.connect(
        user=conn.login,
        password=conn.password,
        account=extra.get('account'),
        warehouse=extra.get('warehouse'),
        database=extra.get('database'),
        schema=extra.get('schema'),
        role=extra.get('role', 'PUBLIC')
    )
    
    # 5. Crear la tabla si no existe (con todas las columnas)
    cursor = ctx.cursor()
    try:
        # Verificar si la tabla existe, si no, crearla
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS STG_PREDICTIT_MARKETS_ENRICHED (
                -- Datos del mercado (nivel principal)
                MARKET_ID STRING,
                MARKET_NAME STRING,
                MARKET_SHORT_NAME STRING,
                MARKET_URL STRING,
                MARKET_TIMESTAMP STRING,
                MARKET_STATUS STRING,
                MARKET_LAST_TRADE_PRICE NUMBER(10,4),
                MARKET_BEST_BUY_YES_COST NUMBER(10,4),
                MARKET_BEST_SELL_YES_COST NUMBER(10,4),
                MARKET_BEST_BUY_NO_COST NUMBER(10,4),
                MARKET_BEST_SELL_NO_COST NUMBER(10,4),
                MARKET_LAST_CLOSE_PRICE NUMBER(10,4),
                
                -- Datos del contrato (nivel anidado)
                CONTRACT_ID STRING,
                CONTRACT_NAME STRING,
                CONTRACT_SHORT_NAME STRING,
                CONTRACT_LAST_TRADE_PRICE NUMBER(10,4),
                CONTRACT_BEST_BUY_YES_COST NUMBER(10,4),
                CONTRACT_BEST_SELL_YES_COST NUMBER(10,4),
                CONTRACT_BEST_BUY_NO_COST NUMBER(10,4),
                CONTRACT_BEST_SELL_NO_COST NUMBER(10,4),
                CONTRACT_LAST_CLOSE_PRICE NUMBER(10,4),
                CONTRACT_DISPLAY_ORDER NUMBER,
                
                -- Metadatos de la carga
                LOAD_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        logger.info("Tabla STG_PREDICTIT_MARKETS_ENRICHED creada/verificada")
        
        # Contar filas antes de la inserción
        cursor.execute("SELECT COUNT(*) FROM STG_PREDICTIT_MARKETS_ENRICHED")
        count_before = cursor.fetchone()[0]
        logger.info(f"Filas antes de la inserción: {count_before}")
        
        # Insertar en lotes
        insert_sql = """
            INSERT INTO STG_PREDICTIT_MARKETS_ENRICHED (
                MARKET_ID, MARKET_NAME, MARKET_SHORT_NAME, MARKET_URL,
                MARKET_TIMESTAMP, MARKET_STATUS, MARKET_LAST_TRADE_PRICE,
                MARKET_BEST_BUY_YES_COST, MARKET_BEST_SELL_YES_COST, MARKET_BEST_BUY_NO_COST,
                MARKET_BEST_SELL_NO_COST, MARKET_LAST_CLOSE_PRICE,
                CONTRACT_ID, CONTRACT_NAME, CONTRACT_SHORT_NAME,
                CONTRACT_LAST_TRADE_PRICE, CONTRACT_BEST_BUY_YES_COST, CONTRACT_BEST_SELL_YES_COST,
                CONTRACT_BEST_BUY_NO_COST, CONTRACT_BEST_SELL_NO_COST, CONTRACT_LAST_CLOSE_PRICE,
                CONTRACT_DISPLAY_ORDER
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Ejecutar batch insert
        cursor.executemany(insert_sql, rows)
        logger.info(f"Insertadas {len(rows)} filas")
        
        # Verificar después
        cursor.execute("SELECT COUNT(*) FROM STG_PREDICTIT_MARKETS_ENRICHED")
        count_after = cursor.fetchone()[0]
        logger.info(f"Filas después de la inserción: {count_after}")
        
        # Commit explícito
        cursor.execute("COMMIT")
        
    except Exception as e:
        logger.error(f"Error al insertar datos: {e}")
        raise
    finally:
        cursor.close()
        ctx.close()
    
    logger.info("Carga completada exitosamente")