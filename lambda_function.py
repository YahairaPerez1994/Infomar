import pandas as pd
from bs4 import BeautifulSoup
import cloudscraper
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timedelta
import boto3
import io
 
 
def lambda_handler(event, context):
   
    puertos = [
    (2, 'ACAPULCO'), (29, 'ANCON'), (43, 'ATICO'), (139, 'BELEN'), (140, 'CABALLOCOCHA'),
    (7, 'CABO BLANCO'), (30, 'CALLAO'), (137, 'CAMANA'), (3, 'CANCAS'), (26, 'CARQUIN'),
    (55, 'CASMA'), (33, 'CERRO AZUL'), (42, 'CHALA'), (28, 'CHANCAY'), (85, 'CHERREPE'),
    (22, 'CHIMBOTE'), (31, 'CHORRILLOS'), (12, 'CONSTANTE'), (143, 'CONTAMANA'), (25, 'CULEBRAS'),
    (36, 'EL CHACO'), (73, 'EL DORADO'), (6, 'EL AÑURO'), (64, 'GRAU'), (27, 'HUACHO'),
    (51, 'HUARMEY'), (145, 'IBERIA'), (47, 'ILO'), (102, 'ISLAY'), (9, 'ISLILLA'), (63, 'LA CRUZ'),
    (44, 'LA PLANCHADA'), (81, 'LA PUNTILLA'), (38, 'LAGUNA GRANDE'), (37, 'LAGUNILLAS'), (74, 'LAS DELICIAS'),
    (41, 'LOMAS'), (23, 'LOS CHIMUS'), (5, 'LOS ORGANOS'), (19, 'MALABRIGO'), (4, 'MANCORA'),
    (70, 'MATACABALLO'), (46, 'MATARANI'), (49, 'MORRO SAMA'), (18, 'PACASMAYO'), (10, 'PAITA'),
    (13, 'PARACHIQUE'), (16, 'PIMENTEL'), (32, 'PUCUSANA'), (71, 'PUERTO ETEN'), (146, 'PUERTO MALDONADO'),
    (1, 'PUERTO PIZARRO'), (14, 'PUERTO RICO'), (101, 'PUERTO VIEJO'), (86, 'PUERTO'), (138, 'PUNCHANA'),
    (45, 'QUILCA'), (39, 'RANCHERIO'), (141, 'REQUENA'), (21, 'SALAVERRY'), (24, 'SAMANCO'), (35, 'SAN ANDRES'),
    (15, 'SAN JOSE'), (40, 'SAN JUAN DE MARCONA'), (17, 'SANTA ROSA'), (52, 'SUPE'), (8, 'TALARA'), (34, 'TAMBO DE MORA'),
    (53, 'VEGUETA'), (11, 'YACILA'), (142, 'YURIMAGUAS'), (65, 'ZORRITOS'), (144, '07 DE JUNIO')
    ]
   
    client_ssm = boto3.client('ssm')
    s3 = boto3.client('s3')
   
    main_bucket_x = "projetc-infomar"
 
 
    # Usuario y contraseña para la autenticación
    usuario = 'invitado'
    contraseña = 'invitado'
 
    # Crear un scraper con autenticación
    scraper = cloudscraper.create_scraper()
    scraper.headers.update({'User-Agent': 'Mozilla/5.0'})
 
    # Realizar la autenticación
    login_url = 'https://infomar.imarpe.gob.pe/Acceso/Login?ReturnUrl=%2FHome%2FIndex'
    login_data = {'user': usuario, 'password': contraseña}
    login_response = scraper.post(login_url, data=login_data)
 
    # Obtener la fecha actual y las fechas de los últimos tres días
    fecha_actual = datetime.now()
    fechas = pd.date_range(end=fecha_actual, periods=5, freq='D').strftime('%d/%m/%Y').tolist()
 
    # Leer el archivo Parquet existente
    existing_parquet = 'infomar_total.parquet'
    file_key = f'infomar/data-input/{existing_parquet}'  # La ruta en S3 debe ser relativa al bucket
    response = s3.get_object(Bucket=main_bucket_x, Key=file_key)
    file_content = response['Body'].read()
    existing_table = pq.read_table(io.BytesIO(file_content))
 
   
    # Inicializar una lista para almacenar los datos del web scraping
    nuevos_datos = []
 
    # Iterar sobre las fechas
    for fex in fechas:
        # Iterar sobre las filas del archivo de puertos
        for codigo, puerto in puertos:
            # Verificar si la autenticación fue exitosa
            if login_response.ok:
                # Construir la URL de datos con el código del puerto y la fecha
                data_url = f'https://infomar.imarpe.gob.pe/Reportes/EspeciesPuerto?LUGAR_ID={codigo}&fecha={fex}'
                data_response = scraper.get(data_url)
 
                # Crear un objeto BeautifulSoup para analizar el HTML
                soup = BeautifulSoup(data_response.content, 'html.parser')
 
                # Encontrar la tabla de datos
                table = soup.find('table')
 
                # Obtener las filas de la tabla
                filas = table.find_all('tr')
 
                # Iterar sobre cada fila y extraer los datos de las celdas
                for fila in filas:
                    # Obtener las celdas de la fila
                    celdas = fila.find_all('td')
                    # Extraer el texto de cada celda y agregarlo a la lista de datos de la tabla
                    datos_fila = [codigo] + [celda.get_text(strip=True) for celda in celdas] + [fex] + [puerto]
                    if len(datos_fila) > 4:
                        nuevos_datos.append(datos_fila)
 
                print(f'Los datos para la fecha {fex} se han recopilado correctamente.')
 
    # Convertir la lista de nuevos datos a un DataFrame de pandas
    nuevos_df = pd.DataFrame(nuevos_datos, columns=['codigo', 'nombre_comun', 'nombre_cientifico', 'tamanho', 'precio', 'hora', 'fecha', 'puerto'])
    nuevos_df['fecha'] = pd.to_datetime(nuevos_df['fecha'])
    # Concatenar el DataFrame de nuevos datos con el DataFrame existente del archivo Parquet
    merged_df = pd.concat([existing_table.to_pandas(), nuevos_df])
 
    merged_df['codigo'] = pd.to_numeric(merged_df['codigo'], errors='coerce')
    merged_df['precio'] = pd.to_numeric(merged_df['precio'], errors='coerce')
    merged_df['puerto'] = merged_df['puerto'].replace({'EL Ã‘URO': 'EL ÑURO', 'EL AÑURO': 'EL ÑURO'})
    # merged_df['codigo'] = merged_df['codigo'].astype(int)
 
    # Eliminar duplicados basados en todas las columnas
    merged_df = merged_df.drop_duplicates(subset = ['nombre_comun','nombre_cientifico','tamanho','precio','hora','fecha','puerto'], keep='last')
   
    buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(merged_df), buffer)
    # Subir el archivo Parquet a S3
    s3.put_object(Bucket=main_bucket_x, Key='infomar/data-input/infomar_total.parquet', Body=buffer.getvalue())