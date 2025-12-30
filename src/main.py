from pathlib import Path

import clients.s3_client as s3_client

def main():
    base_url = "https://noaa-goes19.s3.amazonaws.com"
    folder_path = "ABI-L1b-RadF/2025/364/03/"

    output_dir = Path(__file__).parent / "test_files"
    output_dir.mkdir(parents=True, exist_ok=True)

    s3 = s3_client.S3Client(base_url)
    
    try:
        files = s3.download_folder(folder_path)
        for file_path, content in files.items():
            dest = output_dir / Path(file_path).name  # Guarda cada archivo en test_files usando solo el nombre
            with open(dest, "wb") as fh:
                fh.write(content)
            print(f"Downloaded {file_path}: {len(content)} bytes -> saved to {dest}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()
