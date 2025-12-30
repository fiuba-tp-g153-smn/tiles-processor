import requests
from typing import Dict, List
from xml.etree import ElementTree as ET

S3_NAMESPACE = {'s3': 'http://s3.amazonaws.com/doc/2006-03-01/'}

class S3Client:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def download_file(self, relative_file_path: str) -> bytes:
        url = f"{self.base_url}/{relative_file_path}"
        response = requests.get(url)
        response.raise_for_status()
        return response.content

    def download_folder(self, folder_path: str) -> Dict[str, bytes]:
        file_paths = self.get_folder_file_paths(folder_path)
        files = {}
        for file_path in file_paths:
            file_content = self.download_file(file_path)
            files[file_path] = file_content
        
        return files
    
    def get_folder_file_paths(self, folder_path: str) -> List[str]:
        file_paths = []
        try:
            url = f"{self.base_url}/?prefix={folder_path}&list-type=2"
            response = requests.get(url)
            response.raise_for_status()
            
            root = ET.fromstring(response.content)
            
            for contents in root.findall('s3:Contents', S3_NAMESPACE):
                key = contents.findtext('s3:Key', '', S3_NAMESPACE)
                if key and not key.endswith('/'):
                    file_paths.append(key)
                    
        except Exception as e:
            raise Exception(f"Error obteniendo rutas de archivos en la carpeta {folder_path}: {str(e)}")
        
        return file_paths