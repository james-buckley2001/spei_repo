import pandas as pd
from pathlib import Path

#eggghjdnvldjvvdk;m
class DataStorage:
    def __init__(self):
        self.possible_acculmulation_periods = [1,3,6,12,18,24] # in months
        self.gld_time_period = [1961,2022] # gld = general logistic distributiom

        self._wd = Path(r'C:\Users\BUCKLEJ1\OneDrive - Jacobs\Documents\SPEI Application\data_from_ea\Task 2 SPI SPEI\Input files')
        self._rainfall_input_data_path = self._wd / r'WSR_HydAreas_HadUK_v1_2_0_0_1871_ForRainfallAnalysis.xlsx'
        self._pet_input_data_path = self._wd / r'WSR_EA-PET_1961_HydAreasForGrassPET_Eng.xlsx' #within hydrological areas


#jbj
class SpeiCalculator(DataStorage):
    """
    Inputs: gridded rainfall data, PET within hydrometric areas data, acculmulation period

    Outputs: SPEI grids
    """
    def __init__(self):
        super().__init__()
        pass

    def import_gridded_rainfall(self):
        self.rainfall_data = pd.read_excel(self._rainfall_input_data_path, header=[0, 1])

    def import_hydrometric_areas_pet_data(self):
        self.pet_data = pd.read_excel(self._pet_input_data_path, header=[0, 1])

    def import_input_data(self):
        self.import_hydrometric_areas_pet_data()
        self.import_gridded_rainfall()

    def aggregate_data_for_acculmulation_period(self, acculmulation_period: list) -> pd.DataFrame: 
        
        pass

    def calculate_water_balance(self): #may not need to do this
        #pet = potential evapotranspiration
        pass

    def standardise_values(self):
        pass #within here we will fit GLD 


if __name__ == '__main__':
    spei_calc = SpeiCalculator()
    spei_calc.import_input_data()
    print('egg')