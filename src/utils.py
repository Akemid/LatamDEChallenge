import zipfile

def unzip_json(file_path:str, output_path:str):
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(output_path)
    print(f'Unzipped {file_path} to {output_path}')