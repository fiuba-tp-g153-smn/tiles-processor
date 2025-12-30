import aiohttp
import asyncio
import logging
from typing import Dict, List
from xml.etree import ElementTree as ET

S3_NAMESPACE = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class S3Client:
    def __init__(self, base_url: str, max_concurrent_downloads: int = 5):
        self.base_url = base_url
        self.max_concurrent_downloads = max_concurrent_downloads
        self.semaphore = None  # Se inicializa en async context

    async def download_file(self, session: aiohttp.ClientSession, relative_file_path: str, retries: int = 3) -> tuple:
        """Descarga un archivo con reintentos automáticos"""
        url = f"{self.base_url}/{relative_file_path}"
        
        for attempt in range(1, retries + 1):
            try:
                async with self.semaphore:
                    async with session.get(url) as response:
                        response.raise_for_status()
                        content = await response.read()
                        logger.info(f"✓ Descargado: {relative_file_path} ({len(content)} bytes)")
                        return relative_file_path, content
            except Exception as e:
                logger.warning(f"⚠ Intento {attempt}/{retries} falló para {relative_file_path}: {str(e)}")
                if attempt == retries:
                    logger.error(f"✗ Error descargando {relative_file_path} después de {retries} intentos. Ignorando archivo.")
                    return relative_file_path, None
                await asyncio.sleep(1)  # Espera 1 segundo antes de reintentar

    async def download_folder(self, folder_path: str) -> Dict[str, bytes]:
        """Descarga todos los archivos de una carpeta de forma asincrónica"""
        self.semaphore = asyncio.Semaphore(self.max_concurrent_downloads)
        
        file_paths = await self.get_folder_file_paths(folder_path)
        logger.info(f"Encontrados {len(file_paths)} archivos en {folder_path}")
        
        files = {}
        async with aiohttp.ClientSession() as session:
            tasks = [self.download_file(session, fp) for fp in file_paths]
            results = await asyncio.gather(*tasks, return_exceptions=False)
            
            for file_path, content in results:
                if content is not None:
                    files[file_path] = content
                # Si content es None, el archivo se ignora (falló después de reintentos)
        
        logger.info(f"Descarga completada: {len(files)}/{len(file_paths)} archivos descargados exitosamente")
        return files
    
    async def get_folder_file_paths(self, folder_path: str) -> List[str]:
        """Obtiene las rutas de archivos en una carpeta de forma asincrónica"""
        file_paths = []
        try:
            url = f"{self.base_url}/?prefix={folder_path}&list-type=2"
            async with aiohttp.ClientSession() as session:
                async with session.get(url) as response:
                    response.raise_for_status()
                    content = await response.read()
            
            root = ET.fromstring(content)
            
            for contents in root.findall('s3:Contents', S3_NAMESPACE):
                key = contents.findtext('s3:Key', '', S3_NAMESPACE)
                if key and not key.endswith('/'):
                    file_paths.append(key)
                    
        except Exception as e:
            logger.error(f"Error obteniendo rutas de archivos en {folder_path}: {str(e)}")
            raise Exception(f"Error obteniendo rutas de archivos en la carpeta {folder_path}: {str(e)}")
        
        return file_paths