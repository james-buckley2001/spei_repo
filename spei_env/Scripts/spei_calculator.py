import pandas as pd
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
from scipy import stats
from scipy.stats import genlogistic
from scipy.stats import logistic
from typing import List

#TODO if no name in first heading then use second heading - or just use second heading?

@dataclass
class DataStorage():
    possible_acculmulation_periods:List[float] = field(default_factory=list)
    gld_time_period : List[float] = field(default_factory=list)
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

        new_columns = [(top, bottom) if 'Unnamed' not in bottom else (top,top) for top, bottom in df.columns] #replacing missing headers in bottom row
        df.columns = pd.MultiIndex.from_tuples(new_columns)

        headers = df.columns
        df.columns = df.columns.droplevel(0) #place names are removed and stored for later matching, place IDs are kept
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
        self.df = df
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
            aggregation_dates_df.apply(lambda row: self.calculate_mean_for_given_date_range(row, output_list = averaged_data), axis = 1)
            time_series_df =  pd.concat(averaged_data, axis=0)
            list_of_time_series.append(time_series_df)
        self.df = None
        return list_of_time_series
    
    def calculate_mean_for_given_date_range(self, row, output_list):
        df = self.df
        df_filtered = df[(df['Date'] <= row['aggregation_start_dates']) & (df['Date'] >= row['aggregation_end_dates'])]
        numeric_mean = df_filtered.mean(numeric_only=True)
        columns_to_keep = df_filtered.columns
        mean_df = pd.DataFrame(columns=columns_to_keep)
        mean_df.loc[0] = numeric_mean
        mean_df.drop(columns=['Year', 'Month', 'Status'], inplace=True)
        mean_df['Date'] = row['aggregation_start_dates']
        output_list.append(mean_df)

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
            water_balance_ts = water_balance_ts.reset_index()
            water_balance_ts_list.append(water_balance_ts)
        self.water_balance_ts_list = water_balance_ts_list

    def standardise_values(self):
        normalised_ts_list = []
        for water_balance_ts in self.water_balance_ts_list:
            water_balance_ts.reset_index(inplace=True)
            water_balance_ts.set_index('Date', inplace=True, drop = True)
            water_balance_ts.drop(columns = ['index'], inplace=True)

            normalised_data_list = []
            #fitting and normalising by GLD
            for column_name, column_data in water_balance_ts.items():
                self.normalise_data_using_gld(data = column_data, 
                                              output_list = normalised_data_list,
                                              column_name = column_name)
                

            normalised_data_df =  pd.concat(normalised_data_list, axis=1)
            normalised_data_df.reset_index(inplace=True)
            normalised_ts_list.append(normalised_data_df)

        normalised_ts_df =  pd.concat(normalised_ts_list, axis=0)
        self.spei_values = normalised_ts_df


    def normalise_data_using_gld(self, data, column_name, output_list):
        _c_estimated, mu_estimated, sigma_estimated = genlogistic.fit(data)
        normalised_data = (data - mu_estimated) / sigma_estimated
        normalised_data.columns = column_name
        output_list.append(normalised_data)



if __name__ == '__main__':
    spei_calc = SpeiCalculator(acculmulation_period = 6)
    spei_calc.import_input_data()

    #print(spei_calc.rainfall_data)
    #print(spei_calc.pet_data)

    spei_calc.aggregate_water_balance_data()
    #print(spei_calc.list_of_time_series_rainfall)
    #print(spei_calc.list_of_time_series_pet)

    spei_calc.calculate_water_balance()
    #print(spei_calc.water_balance_ts_list)

    spei_calc.standardise_values()
    print(spei_calc.spei_values.sort_values(by='Date'))
    #TODO reattach headers, truncate SPEI values at 5?

    print('egg')