import pandas as pd
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILENAME = os.path.join(SCRIPT_DIR, 'filtered_master_annual_air_quality_12_cities.csv')

OUTPUT_FILENAME = os.path.join(SCRIPT_DIR, 'pm25_aggregated_yearly_city.csv')

PM25_PARAMETERS = [
    'PM2.5',                
    'PM2.5 - Filter Gravimetric', 
    'PM2.5 Mass'            
]

def aggregate_pm25_data(input_file, output_file, pm_params):
    """
    Loads raw air quality data, filters for PM2.5, and aggregates it by 
    calculating the mean 'Arithmetic Mean' for each City (CBSA Name) and Year.
    Finally, it sorts the data by City and then by Year.
    """

    print(f"Checking for input file at: {input_file}")
    if not os.path.exists(input_file):
        print(f"Error: Input file '{os.path.basename(input_file)}' not found at the specified location.")
        print("Please ensure the file is in the same directory as this script.")
        return

    print(f"Loading and processing raw data from {os.path.basename(input_file)}...")
    try:

        df = pd.read_csv(input_file, low_memory=False)

        pm25_filter = df['Parameter Name'].str.contains('|'.join(pm_params), case=False, na=False)
        df_pm25 = df[pm25_filter].copy()

        if df_pm25.empty:
            print("Warning: No PM2.5 data found after filtering 'Parameter Name'. Check the list of parameters.")
            return

        df_aggregated = df_pm25.groupby(['Year', 'CBSA Name'])['Arithmetic Mean'].mean().reset_index()

        df_aggregated.rename(columns={
            'CBSA Name': 'City',
            'Arithmetic Mean': 'PM2.5'
        }, inplace=True)
        
        df_aggregated.dropna(subset=['PM2.5'], inplace=True)
        
        df_aggregated.sort_values(by=['City', 'Year'], inplace=True)
        
        df_aggregated.to_csv(output_file, index=False)
        
        print(f"\nAggregation complete. The final file has {len(df_aggregated)} rows.")
        print(f"Saved the aggregated data (one PM2.5 value per City per Year) to '{os.path.basename(output_file)}'.")
        print("\nFinal Columns:", df_aggregated.columns.tolist())

    except Exception as e:
        print(f"An unexpected error occurred during aggregation: {e}")

# --- Execute the function ---
aggregate_pm25_data(INPUT_FILENAME, OUTPUT_FILENAME, PM25_PARAMETERS)
