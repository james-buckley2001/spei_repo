import pandas as pd
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime
from dateutil.relativedelta import relativedelta
import numpy as np

#TODO if no name in first heading then use second heading - or just use second heading?

@dataclass
class DataStorage():
    possible_acculmulation_periods:list[float] = field(default_factory=list)
    gld_time_period : list[float] = field(default_factory=list)
    _wd : Path = Path.cwd() / Path(r'spei_env\InputData')
    _rainfall_input_data_file_name : str = r'WSR_HydAreas_HadUK_v1_2_0_0_1871_ForRainfallAnalysis.xlsx'
    _pet_input_data_file_name : str = r'WSR_EA-PET_1961_HydAreasForGrassPET_Eng.xlsx'
    _start_year : str = r'1961'
    _end_year :str = r'2022'

    def __post_init__(self):
        self.gld_time_period = [1961,2022]
        self.possible_acculmulation_periods = [1,3,6,12,18,24,36]

class SpeiCalculator(DataStorage):
    """
    Inputs: gridded rainfall data, PET within hydrometric areas data, acculmulation period

    Outputs: SPEI grids
    """
    def __init__(self, acculmulation_period:float):
        super().__init__()
        self.acculmulation_period = acculmulation_period
        pass

    def import_data(self, file_name):
        df = pd.read_excel(self._wd / file_name, header=[0, 1])
        if 'year' in df.columns:
            df.rename(columns={'year': 'Year'}, inplace=True)
        if 'month' in df.columns:
            df.rename(columns={'month': 'Month'}, inplace=True)
        headers = df.columns
        df.columns = df.columns.droplevel(1)
        return df, headers

    def import_input_data(self):
        self.rainfall_data, self.rainfall_data_headers =  self.import_data(file_name = self._wd / self._rainfall_input_data_file_name)
        self.pet_data, self.pet_data_headers = self.import_data(file_name = self._wd / self._pet_input_data_file_name)

    def generate_aggregation_dates(self,start_date, end_date):
        start_date_obj = start_date
        dates_list = []
        while start_date_obj <= end_date:
            new_date = start_date_obj.replace(year=start_date_obj.year + 1)
            dates_list.append(new_date)
            start_date_obj = new_date
        return dates_list

    def produce_mean_time_series_one_starting_each_month(self, df = None) -> pd.DataFrame: 
        df['Date'] = pd.to_datetime(df[['Year', 'Month']].assign(day=1))

        list_of_time_series = []
        for month in range(1,13):
            start_date = datetime.strptime(f'{self._start_year}-{month:02d}-01', '%Y-%m-%d')
            end_date = datetime.strptime(f'{self._end_year}-{month:02d}-01', '%Y-%m-%d')
            aggregation_start_dates = self.generate_aggregation_dates(start_date, end_date)
            aggregation_end_dates = [date - relativedelta(months=self.acculmulation_period-1) for date in aggregation_start_dates]
            aggregation_dates_df = pd.DataFrame({
                                        'aggregation_start_dates': aggregation_start_dates,
                                        'aggregation_end_dates': aggregation_end_dates
                                    })
            averaged_data = []
            for _, row in aggregation_dates_df.iterrows():
                df_filtered = df[(df['Date'] <= row['aggregation_start_dates']) & (df['Date'] >= row['aggregation_end_dates'])]
                numeric_mean = df_filtered.mean(numeric_only=True)
                columns_to_keep = df_filtered.columns
                mean_df = pd.DataFrame(columns=columns_to_keep)
                mean_df.loc[0] = numeric_mean
                mean_df.drop(columns=['Year', 'Month', 'Status'], inplace=True)
                mean_df['Date'] = row['aggregation_start_dates']
                averaged_data.append(mean_df)

            time_series_df =  pd.concat(averaged_data, axis=0)
            list_of_time_series.append(time_series_df)

        return list_of_time_series

    def aggregate_water_balance_data(self):
        self.list_of_time_series_rainfall = self.produce_mean_time_series_one_starting_each_month(df = self.rainfall_data)
        self.list_of_time_series_pet = self.produce_mean_time_series_one_starting_each_month(df = self.pet_data)

    def align_two_dataframes(self, df_rainfall, df_pet):
            common_columns = df_rainfall.columns.intersection(df_pet.columns)
            df_rainfall = df_rainfall[common_columns]
            df_pet = df_pet[common_columns]
            df_rainfall, df_pet  = df_rainfall.align(df_pet, join='outer', axis=None, fill_value=np.nan)
            return df_rainfall, df_pet


    def calculate_water_balance(self): #code needs to ensure correct columns are deducted
        water_balance_ts_list = []
        for month in range(1,13):
            index = month -1

            self.list_of_time_series_pet[index].set_index('Date', inplace=True)
            self.list_of_time_series_rainfall[index].set_index('Date', inplace=True)

            rainfall_ts, pet_ts = self.align_two_dataframes(self.list_of_time_series_rainfall[index], 
                                                       self.list_of_time_series_pet[index])

            water_balance_ts = rainfall_ts.sub(pet_ts, fill_value=np.nan)

            water_balance_ts_list.append(water_balance_ts)

        self.water_balance_ts_list = water_balance_ts_list

    def standardise_values(self):
        pass #within here we will fit GLD 


if __name__ == '__main__':
    spei_calc = SpeiCalculator(acculmulation_period= 6)
    spei_calc.import_input_data()
    spei_calc.aggregate_water_balance_data()
    spei_calc.calculate_water_balance()
    print('egg')