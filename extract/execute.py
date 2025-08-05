import os,sys,requests
from zipfile import ZipFile

def download_zip_file(url, output_dir):
    response = requests.get(url, stream=True)
    os.makedirs(output_dir, exist_ok=True)
    if response.status_code == 200:
        filename = os.path.join(output_dir, "downloaded.zip")
        with open(filename, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        print(f"Downloaded zip file : {filename}")
        return filename
    else:
        raise Exception(f"Failed to download file. Status code: {response.status_code}")
    

def extract_zip_file(zip_filename, output_dir):

    with ZipFile(zip_filename, "r") as zip_file:
        zip_file.extractall(output_dir)

    print(f"Extracted files written to: {output_dir}")
    print("Removing the zip file")
    os.remove(zip_filename)

def fix_json_dict(output_dir):
    import json
    file_path = os.path.join(output_dir, "dict_artists.json")
    with open(file_path, "r") as f:
        data = json.load(f)

    with open(os.path.join(output_dir, "fixed_da.json"), "w", encoding="utf-8") as f_out:
        for key, value in data.items():
            record = {"id": key, "related_ids": value}
            json.dump(record, f_out, ensure_ascii=False)
            f_out.write("\n")
    print(f"File {file_path} has been fixed and written to {output_dir} as fixed_da.json")
    print("Removing the original file")
    os.remove(file_path)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Extraction path is required")
        print("Example Usage:")
        print("python3 execute.py /home/ardent-sharma/Data/Extraction")
    else:
        try:
            print("Starting Extraction Engine...")
            EXTRACT_PATH = sys.argv[1]
            KAGGLE_URL = "https://storage.googleapis.com/kaggle-data-sets/1993933/3294812/bundle/archive.zip?X-Goog-Algorithm=GOOG4-RSA-SHA256&X-Goog-Credential=gcp-kaggle-com%40kaggle-161607.iam.gserviceaccount.com%2F20250728%2Fauto%2Fstorage%2Fgoog4_request&X-Goog-Date=20250728T025229Z&X-Goog-Expires=259200&X-Goog-SignedHeaders=host&X-Goog-Signature=4f3223c142f4d348b43fe415eba5f89222464d1ded84c426b8269374618165ac3a9312239f18ee760e6d346ab04ac599fedd48682512d44ed430f51fa5147e61617b537f9accd5182677ddc717c490fb8720cd6110ed5ec552fa8475d7788729500b19bff00b5df19acca9616a498e8a2bf03f15b1e2727963133fd83be22247d42da152eb1dbbc0b2ecc3e00eca42bdf862facbd05a871186f99ea7428b4ead32c0bbcec459c8409a42f403a65dd5541cf705e18fd7baf5f23235bf08137f89655491252b4300f8f0513f62683a18f3a105ae8955b4343640ddd2b86f835b819e370fee5a4291ca873e114a1d0c39eeb737ca6bd517d088c87e1d8a22884c8c"
            zip_filename = download_zip_file(KAGGLE_URL, EXTRACT_PATH)
            extract_zip_file(zip_filename, EXTRACT_PATH)
            fix_json_dict(EXTRACT_PATH)
            print("Extraction Successfully Completed!!!")
        except Exception as e:
            print(f"Error: {e}")

