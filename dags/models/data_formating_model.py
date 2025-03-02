import pandas as pd

def data_extraction():
    print("Extracting super store data from the machine")
    
    # Define the path to the CSV file
    data_path = '/mnt/c/Users/takue/Documents/Data Science/Super Store/Sample Superstore - Orders.csv'
    
    try:
        # Read the CSV file into a pandas DataFrame
        data = pd.read_csv(data_path)
        print("Data extracted successfully")
        print(f"Data shape: {data.shape}")  
        return data 
    except FileNotFoundError:
        print(f"Error: The file at {data_path} was not found.")
    except Exception as e:
        print(f"An error occurred while extracting the data: {e}")

extracted_data = data_extraction()

# Check if data was extracted successfully
if extracted_data is not None:
    print("Data preview:")
    print(extracted_data.head())  