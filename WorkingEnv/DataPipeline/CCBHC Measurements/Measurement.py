from abc import ABC, abstractmethod
from Numerator import Numerator
from Denominator import Denominator
import pandas as pd

class Submeasure(Denominator,Numerator):
    """
    A base class for all submeasure calculations.

    This class defines the core methods that must be implemented 
    by any concrete measurement class. 

    Inherits:
        Denominator: Provides methods for retrieving and processing the denominator data.
        Numerator: Provides methods for retrieving and processing the numerator data.
    """

    def __init__(self,name):
        super().__init__()
        self.__NAME__:str = name
        self.__populace__:pd.DataFrame = None
        self.__stratify__:pd.DataFrame = None

    def get_name(self) -> str:
        """
        Returns the name of the submeasure
        """
        return self.__NAME__
    
    def collect_measurement_data(self) -> tuple[pd.DataFrame,pd.DataFrame]:
        """
        Calls all functions of the measurement

        Returns:
            tuple[pd.DataFrame, pd.DataFrame]:

                - The first DataFrame represents the processed populace data
                - The second DataFrame represents the processed stratified data
        """
        self.get_denominator()
        self.get_numerator()
        self.stratify_data()
        return self.return_final_data()

    @abstractmethod
    def get_populace_dataframe(self):
        """
        Returns the populace dataframe

        This method must be implemented by the concrete class
        """
        pass

    @abstractmethod
    def get_stratify_dataframe(self):
        """
        Returns the stratify dataframe

        This method must be implemented by the concrete class
        """
        pass

    def get_denominator(self):
        """
        Retrieves the data for the denominator of the measurement.
        """
        self._get_populace()
        self._remove_exclusions()

    def get_numerator(self):
        """
        Retrieves the data for the numerator of the measurement.
        """
        self._apply_time_constraint()
        self._find_performance_met()

    @abstractmethod
    def stratify_data(self):
        """
        Stratifies the data for the measurement.

        This method must be implemented by the concrete class 
        to define how the data is stratified (e.g., by age, gender).
        """
        pass
    

    @abstractmethod
    def return_final_data(self)-> tuple[pd.DataFrame,pd.DataFrame]:
        """
        Returns the calculated measurement data.

        This method must be implemented by the concrete class 
        to define how the calculated measurement is stored.
        """
        pass

class Measurement(ABC):
    """
    A standardized metric created by SAMHSA used to measure the performance of a CCBHC
    """

    def __init__(self,name):
        super().__init__()
        self.__NAME__:str = name

    def get_name(self) -> str:
        """
        Returns the name of the Measurement
        """
        return self.__NAME__

    @abstractmethod
    def get_submeasure_data(self) -> dict[str,pd.DataFrame]:
        """
        Calculates all the data for the Measurement and its Submeasures

        Returns:
            Dictionary[str,pd.DataFrame]
            - str: The name of the submeasure data
            - pd.DataFrame: The data corresponding to that submeasure
        """
        pass