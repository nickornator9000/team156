import os
import sys

def download_dataset()->None:
    
    # Define the dataset path on Kaggle
    dataset_path = 'rtatman/188-million-us-wildfires/2'

    # Define the path where you want to download the dataset
    download_path = './data'
    
    # Check if a key dataset file already exists to avoid re-downloading
    key_file_path = os.path.join(download_path, 'FPA_FOD_20170508.sqlite')
    if os.path.exists(key_file_path):
        print(f"Dataset already exists at {key_file_path}. Skipping download.")
        return
    else:
        # Make sure the download path exists
        import kaggle
        os.makedirs(download_path, exist_ok=True)

        # Use the Kaggle API to download the dataset
        kaggle.api.dataset_download_files(dataset_path, path=download_path, unzip=True)
        print("Dataset downloaded successfully.")
        return

def getUserLogin():
    #function to pass user login to env for kaggle api
    pass

if len(sys.argv) != 3:
    print("Error: your kaggle username and key are required.")
    print("Usage: python build/scripts/download_dataset.py your_kaggle_username your_kaggle_key")
    sys.exit(1)
        
kaggle_username = sys.argv[1]
kaggle_key = sys.argv[2]

print("Setting Kaggle account credentials")
print(f"Username: {kaggle_username}")
print(f"Key: {kaggle_key}")

os.environ['KAGGLE_USERNAME'] = sys.argv[1]
os.environ['KAGGLE_KEY'] = sys.argv[2]

download_dataset()