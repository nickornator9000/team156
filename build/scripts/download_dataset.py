import os
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

if __name__ == '__main__':
    print("hello world")