from data_cleaning import DestatisData
import pandas as pd

class DataProcessor:
    def __init__(self, enrolled_students_file, cpi_file):
        self.enrolled_students_file = enrolled_students_file
        self.cpi_file = cpi_file
        self.enrolled_students_data = None
        self.cpi_data = None
        self.merged_data = None

    def load_enrolled_students_data(self):
        """
        Loads and cleans the enrolled students data from the Destatis dataset.
        """
        es = DestatisData(self.enrolled_students_file, header=0)
        
        # Filter relevant columns and rows
        es.df = es.df[['time', 'value', '2_variable_attribute_code', '3_variable_attribute_label']]
        es.df = es.df[es.df['3_variable_attribute_label'] == 'Total']

        # Rename columns
        es.df = es.df.rename(columns={'time': 'year'}, errors='raise')

        # Clean and convert columns to numeric
        es.df['year'] = es.df['year'].str.split('-').str[0]  # Keep only the year part
        es.df['value'] = es.df['value'].replace('-', '')  # Replace hyphens with empty values
        es.df['year'] = pd.to_numeric(es.df['year'], errors='coerce')
        es.df['value'] = pd.to_numeric(es.df['value'], errors='coerce')

        # Separate data into Germans (NATD) and Foreigners (NATA)
        es_natd = es.df[es.df['2_variable_attribute_code'] == 'NATD']
        es_nata = es.df[es.df['2_variable_attribute_code'] == 'NATA']

        # Aggregate the data by 'year'
        es_natd_aggregated = es_natd.groupby("year")['value'].sum().reset_index()
        es_nata_aggregated = es_nata.groupby("year")['value'].sum().reset_index()

        # Merge the two datasets
        es_aggregated = pd.merge(es_natd_aggregated, es_nata_aggregated, on="year", suffixes=('_NATD', '_NATA'))
        es_aggregated['NAT'] = es_aggregated['value_NATD'] + es_aggregated['value_NATA']

        self.enrolled_students_data = es_aggregated

    def load_cpi_data(self):
        """
        Loads and cleans the Consumer Price Index (CPI) data from the Destatis dataset.
        """
        cpi = DestatisData(self.cpi_file, header=0)
        
        # Filter for 'Consumer price index'
        cpi.df = cpi.df[cpi.df['value_variable_label'] == 'Consumer price index']
        
        # Filter relevant columns
        cpi.df = cpi.df[['time', 'value']]

        # Rename columns
        cpi.df = cpi.df.rename(columns={'time': 'year', 'value': 'cpi'}, errors='raise')

        # Convert 'year' to numeric
        cpi.df['year'] = pd.to_numeric(cpi.df['year'], errors='coerce')

        # Convert 'cpi' column to float after ensuring it's a string and removing any non-numeric characters
        cpi.df['cpi'] = pd.to_numeric(cpi.df['cpi'], errors='coerce')

        # Calculate the CPI factor (percentage change)
        cpi.df['cpi_factor'] = cpi.df['cpi'].pct_change()

        self.cpi_data = cpi.df

    def merge_data(self):
        """
        Merges the CPI data with the enrolled students data on the 'year' column.
        """
        if self.cpi_data is not None and self.enrolled_students_data is not None:
            # Merge on 'year'
            self.merged_data = pd.merge(self.cpi_data, self.enrolled_students_data, on="year", how="inner")  # 'inner' keeps only matching years
        else:
            raise ValueError("Both CPI and Enrolled Students data need to be loaded before merging.")

    def get_merged_data(self):
        """
        Returns the merged dataset.
        """
        if self.merged_data is not None:
            return self.merged_data
        else:
            raise ValueError("Data has not been merged yet.")


# Create an instance of DataProcessor
data_processor = DataProcessor("21311-0002_en_flat", "61111-0001_en_flat")

# Load the data
data_processor.load_enrolled_students_data()
data_processor.load_cpi_data()

# Merge the data
data_processor.merge_data()

# Get and print the merged result
merged_result = data_processor.get_merged_data()
print(merged_result)
