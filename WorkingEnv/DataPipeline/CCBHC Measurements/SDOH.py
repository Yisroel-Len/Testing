from typing import override
from sqlalchemy import Engine
from datetime import datetime
from logging import Logger
from Measurement import Measurement, Submeasure
import pandas as pd

class _Sub_1(Submeasure):
    '''
    The Percentage of clients 18 years and older screened for food
    insecurity, housing instability, transportation needs,
    utility difficulties, and interpersonal safety
    '''

    def __init__(self,engine:Engine,logger:Logger):
        super().__init__("SDOH")
        self.__CONN__ = engine
        self.__LOGGER__ = logger

    @override
    def get_populace_dataframe(self) -> pd.DataFrame:
        """
        Gets the populace dataframe 
        Returns: populace
        """
        return self.__populace__.copy()

    @override
    def get_stratify_dataframe(self) -> pd.DataFrame:
        """
        Gets the stratify dataframe 
        Returns: stratify
        """
        return self.__stratify__.copy()

    @override
    def get_denominator(self) -> None:
        '''All clients who were seen within the measurement year and were 18 or older at the time of service'''
        self.__LOGGER__.info("Getting Denominator")
        try:
            super().get_denominator()
            self.__LOGGER__.info("Successfully got Denominator")
        except Exception:
            self.__LOGGER__.error('Failed to get Denominator',exc_info=True)
            raise


    @override
    def _get_populace(self) -> None:
        '''Identify clients seen during the Measurement Year and creates their measurement year ID'''
        self.__initialize_populace()
        self.__populace__['patient_measurement_year_id'] = self.__create_measurement_year_id(self.__populace__['PatientId'],self.__populace__['VisitDateTime'])

    def __initialize_populace(self) -> pd.DataFrame:
        '''Queries db for clients seen during the Measurement Year'''
        sql = '''
        SELECT
            tblEncounterSummary.PatientId,
            tblEncounterSummary.EncounterId,
            tblEncounterSummary.VisitDateTime,
            emr_PatientDetails.DOB
        FROM
            tblEncounterSummary
        LEFT JOIN
            emr_PatientDetails ON (tblEncounterSummary.PatientId = emr_PatientDetails.PatientId)
        LEFT JOIN
            tblEncounterTypeCPTMap ON (tblEncounterSummary.EncounterTypeId = tblEncounterTypeCPTMap.EncounterTypeID)
        WHERE
            YEAR(tblEncounterSummary.VisitDateTime) >= 2024
            AND
            tblEncounterTypeCPTMap.CPTCode IN ('59400', '59510', '59610', '59618', '78012', '78070', '78075', '78102', '78140',
                                               '78185', '78195', '78202', '78215', '78261', '78290', '78300', '78305', '78315',
                                               '78414', '78428', '78456', '78458', '78579', '78580', '78582', '78597', '78601',
                                               '78630', '78699', '78708', '78725', '78740', '78801', '78803', '78999', '90791',
                                               '90792', '90832', '90834', '90837', '90839', '90845', '90945', '90947', '90951',
                                               '90952', '90953', '90954', '90955', '90956', '90957', '90958', '90959', '90960',
                                               '90961', '90962', '90963', '90964', '90965', '90966', '90967', '90968', '90969',
                                               '90970', '92002', '92004', '92012', '92014', '92507', '92508', '92521', '92522',
                                               '92523', '92524', '92526', '92537', '92538', '92540', '92541', '92542', '92544',
                                               '92545', '92548', '92549', '92550', '92557', '92567', '92568', '92570', '92588',
                                               '92625', '92626', '92650', '92651', '92652', '92653', '96116', '96156',
                                               '96158', '97129', '97161', '97162', '97163', '97164', '97802', '97803', '97804',
                                               '98960', '98961', '98962', '99203', '99204', '99205', '99211', '99212', '99213',
                                               '99214', '99215', '99221', '99222', '99223', '99231', '99232', '99233', '99236',
                                               '99242', '99243', '99244', '99245', '99281', '99282', '99283', '99284',
                                               '99285', '99304', '99305', '99306', '99307', '99308', '99309', '99310',
                                               '99381', '99382', '99383', '99384', '99385', '99386', '99387',
                                               '99391', '99392', '99393', '99394', '99395', '99396', '99397',
                                               '99401', '99402', '99403', '99404', '99411', '99412', '99429',
                                               '99495', '99496', '99512', 'D0120', 'D0140', 'D0145', 'D0150',
                                               'D0160', 'D0170', 'D0180', 'D7111', 'D7140', 'D7210', 'D7220',
                                               'D7230', 'D7240', 'D7241', 'D7250', 'D7251', 'G0101', 'G0108', 'G0270',
                                               'G0271', 'G0402', 'G0438', 'G0439', 'G0447', 'G0473', 'G9054'
                                               )
        '''
        self.__populace__ = pd.read_sql(sql,self.__CONN__)

    def __create_measurement_year_id(self, patient_id:pd.Series, date:pd.Series) -> pd.Series:
        '''Creates a unique id to match patients to their coresponding measurement year \n
        Parameters: patient_id
                            date \n
        Returns: patient_measurement_id'''
        return patient_id.astype(str) + '-' + (date.dt.year).astype(str)

    @override
    def _remove_exclusions(self) -> None:
        '''Filters exclusions from populace'''
        # Denominator Exclusions:
        # All clients aged 17 years or younger
        self.__remove_age_exclusion()

    def __remove_age_exclusion(self) -> None:
        '''Finds and reomves all encounters of clients aged 17 years or younger'''
        self.__calculate_age()
        self.__filter_age()
        self.__get_first_encounter()

    def __calculate_age(self) -> None:
        '''Calculates age of client at the date of service'''
        self.__populace__['age'] = (self.__populace__['VisitDateTime'] - self.__populace__['DOB']).dt.days // 365.25

    def __filter_age(self) -> None:
        '''Removes all clients aged 17 or younger at the date of service'''
        self.__populace__ = self.__populace__[self.__populace__['age'] >= 18]

    def __get_first_encounter(self) -> None:
        '''Filters down all encounters to the first encounter per client per year'''
        # needed for clients who turned 18 in the middle of the measurement year, as screening while they were 17 in that measurement year wouldn't count
        self.__populace__ = self.__populace__.sort_values(by=['patient_measurement_year_id','VisitDateTime']).drop_duplicates('patient_measurement_year_id',keep='first')



    @override
    def get_numerator(self):
        '''Doc String'''
        self.__LOGGER__.info('Getting Numerator')
        try:
            self._find_performance_met()
            self._apply_time_constraint()
            self.__LOGGER__.info("Successfully got Numerator")
        except Exception:
            self.__LOGGER__.error('Failed to get Denominator',exc_info=True)
            raise

    @override
    def _find_performance_met(self):
        '''Doc String'''
        screenings = self.__get_screenings()
        screenings['patient_measurement_year_id'] = self.__create_measurement_year_id(screenings['PatientId'],screenings['screening_date'])
        screenings = self.__get_last_screening(screenings)
        self.__merge_screenings_into_populace(screenings)

    def __get_screenings(self):
        '''Doc String'''
        sql = f'''
        SELECT
            DF_SocialNeedsScreeningTool.PatientId,
            DF_SocialNeedsScreeningTool.EncounterId AS 'screening_id',
            tblEncounterSummary.VisitDateTime AS 'screening_date'
        FROM
            DF_SocialNeedsScreeningTool
        LEFT JOIN
            tblEncounterSummary ON (DF_SocialNeedsScreeningTool.EncounterId = tblEncounterSummary.EncounterId)
        WHERE
            DF_SocialNeedsScreeningTool.PatientId IN {tuple(self.__populace__['PatientId'].tolist())} -- creates a list of valid PatientIds
            AND
            YEAR(tblEncounterSummary.VisitDateTime) >= 2024
        '''
        return pd.read_sql(sql,self.__CONN__)

    def __get_last_screening(self,screenings):
        '''Doc String'''
        # patients might have multiple screenings, and patients who turn 18 during the measurement year need to have the screening after their birthday
        return screenings.sort_values(by=['screening_date'],ascending=False).drop_duplicates(['patient_measurement_year_id'],keep='first')

    def __merge_screenings_into_populace(self,screenings):
        '''Doc String'''
        self.__populace__ = self.__populace__.merge(screenings[['screening_id','screening_date','patient_measurement_year_id']],how='left')

    @override
    def _apply_time_constraint(self):
        '''Doc String'''
        self.__set_numerator()

    def __set_numerator(self):
        '''Doc String'''
        # since there is a different logic for patients who are 18 vs 19+
        # populace gets split into 2 dfs to keep O(1) and avoiding df.apply() O(n)
        # for patients that turned 18 in the middle of the year need to have the screening after thier birthday
        age_18 = self.__populace__[self.__populace__['age'] == 18].copy()
        age_18['numerator'] = (age_18['screening_date'] >= (age_18['DOB'] + pd.DateOffset(years=18)))
        # for patients that are over 18 can have the screening at any point
        over_18 = self.__populace__[self.__populace__['age'] > 18].copy()
        over_18['numerator'] = over_18['screening_date'].notna()
        self.__populace__ = pd.concat([age_18,over_18])


   
    @override
    def stratify_data(self) -> None:
        '''Gets stratification for all clients: age, medicaid, ethnicity and race'''
        self.__LOGGER__.info('Getting Stratification')
        try:
            self.__initialize_stratify()
            self.__get_stratify_from_db()
            self.__fill_blank_stratify()
            self.__LOGGER__.info("Successfully got Stratification")
        except Exception:
            self.__LOGGER__.error('Failed to get Stratification',exc_info=True)
            raise

    def __initialize_stratify(self) -> None:
        '''Initializes self.__stratify__ by filtering self.__populace__'''
        # use populace to initialize stratify instead of index_visits because populace is filtered and index_visits still has exclusions in it
        self.__stratify__ = self.__populace__[['patient_measurement_year_id','PatientId','EncounterId','VisitDateTime','screening_date']].sort_values(['patient_measurement_year_id']).copy()
        self.__stratify__['measurement_year'] = self.__stratify__['patient_measurement_year_id'].str.split('-',expand=True)[1]

    def __get_stratify_from_db(self) -> None:
        '''Gets stratification data from the database (race, ethnicity, medicaid)'''
        self.__get_patient_data()
        self.__get_encounter_data()

    def __get_patient_data(self) -> None:
        '''Gets patient stratifications from database (ethnitity and race)'''
        # NOTE
        # if you want to make the entire code more dynamic and to be able to work for other companies and thier databases,
        # then this (and all SQL statements) should be replaced with factory methods
        # i.e. sql = ASCSQLFactory.get_patient_data('PesachTikvah')
        sql = f'''
            SELECT
                emr_PatientDetails.PatientId,
                emr_PatientDetails.EthnicityGroupName AS 'Ethnicity',
                emr_PatientRaceGroupTransaction.RaceGroupName AS 'Race'
            FROM
                emr_PatientDetails
            LEFT JOIN
                emr_PatientRaceGroupTransaction ON (emr_PatientDetails.PatientId = emr_PatientRaceGroupTransaction.PatientId)
            WHERE
                emr_PatientDetails.PatientId IN {tuple(self.__stratify__['PatientId'].tolist())} -- creates a list of valid PatientIds
            '''
        patient_data = pd.read_sql(sql,self.__CONN__).drop_duplicates('PatientId',keep='first')
        self.__stratify__ = self.__stratify__.merge(patient_data,how='left')

    def __get_encounter_data(self) -> None:
        '''Gets encounter stratifications from database (medicaid)'''
        medicaid_data = self.__get_medicaid_from_db()
        medicaid_data = self.__merge_mediciad_with_stratify(medicaid_data)
        medicaid_data = self.__filter_insurance_dates(medicaid_data)
        medicaid_data['patient_measurement_year_id'] = self.__create_measurement_year_id(medicaid_data['PatientId'],medicaid_data['VisitDateTime'])
        results = self.__determine_medicaid_stratify(medicaid_data)
        self.__stratify__ = self.__stratify__.merge(results,how='left')
        # patients that don't have any valid insurtance at their encounter date get completly filtered out and have a NaN instead of False
        # and would otherwise be filled with 'Unknown' by __fill_blank_stratify()
        self.__stratify__['Medicaid'] = self.__stratify__['Medicaid'].fillna(False).copy()

    def __get_medicaid_from_db(self) -> pd.DataFrame:
        '''Queries data base for all relevant patients' insurance information'''
        # NOTE
        # if you want to make the entire code more dynamic and to be able to work for other companies and thier databases,
        # then this (and all SQL statements) should be replaced with factory methods
        # i.e. sql = ASCSQLFactory.get_medicaid('PesachTikvah')
        sql = f'''
            Select
                PatientId,
                EffectiveDate AS 'Start',
                DisenrollmentDate AS 'End',
                LOWER ([tblPayerPlans.PayerPlanName]) AS 'Plan'
            FROM
                tblPatientPayers
            WHERE
                PatientId IN {tuple(self.__stratify__['PatientId'].to_list())} -- creates a list of valid PatientIds
            '''
        return pd.read_sql(sql,self.__CONN__)

    def __merge_mediciad_with_stratify(self,medicaid_data:pd.DataFrame) -> pd.DataFrame:
        '''Merges stratify data on top of the medicaid data \n
        Returns: merged_data'''
        return medicaid_data.merge(self.__stratify__[['PatientId','screening_date','VisitDateTime']],how='left')

    def __filter_insurance_dates(self,medicaid_data:pd.DataFrame) -> pd.DataFrame:
        '''Removes insurances that weren't active at the time of the patient's visit \n
        Returns: valid_medicaid'''
        # replace nulls with today so that they don't get filtered out
        medicaid_data['End'] = medicaid_data['End'].fillna(datetime.now())        
        # medicaid_data.to_clipboard()
        # split medicaid in half so that patients without screenings don't get filtered out
        # the date comparison should use the screening date if it exists else use encounter date
        # by spliting the df O(n) remains constant and avoids df.apply()
        screening_visits = medicaid_data[medicaid_data['screening_date'].notna()].copy()
        encounter_visits = medicaid_data[medicaid_data['screening_date'].isna()].copy()
        screening_visits['valid'] = (screening_visits['Start'] <= screening_visits['screening_date']) & (screening_visits['End'] >= screening_visits['screening_date']) # checks if the insurance is valid at time of screenimg
        encounter_visits['valid'] = (encounter_visits['Start'] <= encounter_visits['VisitDateTime']) & (encounter_visits['End'] >= encounter_visits['VisitDateTime']) # checks if the insurance is valid at time of encounter
        medicaid_data = pd.concat([screening_visits,encounter_visits]).sort_values(['PatientId','VisitDateTime']).copy()
        return medicaid_data[medicaid_data['valid']].copy()

    def __determine_medicaid_stratify(self, medicaid_data:pd.DataFrame) -> pd.DataFrame:
        '''Finds patients that have medicaid only for insurance \n
        Returns: medicaid_data'''
        medicaid_data['Medicaid'] = self.__find_plans_with_medicaid(medicaid_data['Plan'])
        medicaid_data['Medicaid'] = self.__replace_medicaid_values(medicaid_data['Medicaid'])
        medicaid_data = self.__find_patients_with_only_medicaids(medicaid_data)
        return medicaid_data

    def __find_plans_with_medicaid(self,plan:pd.Series) -> pd.Series:
        ''' Checks if the insurance name contains medicaid \n
        Returns: has_medicaid'''
        return plan.str.contains('medicaid')
    
    def __replace_medicaid_values(self, col:pd.Series) -> pd.Series:
        '''Replaces Boolean values with numerical values \n
        Returns: numerical_val'''
        return col.map({True:1,False:2})

    def __find_patients_with_only_medicaids(self,medicaid_data:pd.DataFrame) -> pd.DataFrame:
        '''Calcutlates whether a patient has medicaid only or other insurance \n
        Returns: encounter_ids'''
        medicaid_data = medicaid_data.merge(self.__stratify__,on=['patient_measurement_year_id'],how='left')
        return (medicaid_data.groupby(['patient_measurement_year_id'])['Medicaid'].sum() == 1).reset_index()

    def __fill_blank_stratify(self) -> None:
        '''Fill in all null values with Unknown'''
        self.__stratify__ = self.__stratify__.fillna('Unknown')




    @override
    def return_final_data(self) -> tuple[pd.DataFrame,pd.DataFrame]:
        '''Returns the final calculated data for the SDOH measurement \n
        Returns: \n
            tuple[pd.DataFrame, pd.DataFrame]: \n
                - The first DataFrame represents the processed populace data
                - The second DataFrame represents the processed stratified data'''
        self.__trim_unnecessary_populace_data()
        self.__trim_unnecessary_stratify_data()
        return self.__populace__.copy(), self.__stratify__.copy()

    def __trim_unnecessary_populace_data(self) -> None:
        '''Gets rid of all data that isn't needed for the denominator'''
        self.__populace__ = self.__populace__[['patient_measurement_year_id','PatientId','EncounterId','numerator','screening_id','screening_date']].drop_duplicates(subset='patient_measurement_year_id')

    def __trim_unnecessary_stratify_data(self) -> None:
        '''Gets rid of all data that isn't needed to stratify the populace'''
        self.__stratify__ = self.__stratify__[['patient_measurement_year_id','measurement_year','Ethnicity','Race','Medicaid']].drop_duplicates(subset='patient_measurement_year_id')




class SDOH(Measurement):
    """
    The SDOH measure calculates the Percentage of clients 18 years and older screened for food
    insecurity, housing instability, transportation needs, utility difficulties, and interpersonal safety
    """

    def __init__(self,engine:Engine,logger:Logger):
        super().__init__("SDOH")
        self.__sub1__:Submeasure = _Sub_1(engine,logger)
    
    @override
    def get_submeasure_data(self) -> dict[str,pd.DataFrame]:
        """
        Calculates all the data for the SDOH Measurement and its Submeasures

        Returns:
            Dictionary[str,pd.DataFrame]
                - str: The name of the submeasure data
                - pd.DataFrame: The data corresponding to that submeasure
        """
        results = {}
        pop,strat = self.__sub1__.collect_measurement_data()
        results[self.__sub1__.get_name()] = pop
        results[self.__sub1__.get_name()+'_stratify'] = strat
        return results
        