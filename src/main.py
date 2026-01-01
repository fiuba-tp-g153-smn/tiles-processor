import asyncio
from pathlib import Path

import clients.s3_client as s3_client

async def main():
    bucket_name = "noaa-goes19"
    folder_path = "ABI-L1b-RadF/2026/001/21/"

    output_dir = Path(__file__).parent / "test_files"
    output_dir.mkdir(parents=True, exist_ok=True)

    s3 = s3_client.S3Client(bucket_name, max_concurrent_downloads=5)
    
    try:
        files = await s3.download_folder(folder_path, file_pattern="C13_")
        for file_path, content in files.items():
            dest = output_dir / Path(file_path).name
            with open(dest, "wb") as fh:
                fh.write(content)
            print(f"Saved to {dest}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
