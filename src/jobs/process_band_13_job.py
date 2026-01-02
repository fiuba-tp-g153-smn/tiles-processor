from pathlib import Path

from constants import constants
from clients import s3_client


class ProcessBand13Job:
    def __init__(self):
        self.__bucket_name = constants.GOES19_BUCKET_NAME
        self.__l1b_products_path = "ABI-L1b-RadF/2026/001/21/"
        self.__product_file_pattern = "C13_"
        self.__s3_client = s3_client.S3Client(
            self.__bucket_name, max_concurrent_downloads=5
        )

    async def run(self):
        output_dir = Path(__file__).parent / "test_files"
        output_dir.mkdir(parents=True, exist_ok=True)

        downloaded_files = await self.__s3_client.download_folder(
            self.__l1b_products_path, file_pattern=self.__product_file_pattern
        )
        for file_path, content in downloaded_files.items():
            dest_path = output_dir / Path(file_path).name
            with open(dest_path, "wb") as dest_file:
                dest_file.write(content)
