from typing import override
from sqlalchemy import Engine
from datetime import datetime
from dateutil.relativedelta import relativedelta
from logging import Logger
from Measurement import Measurement, Submeasure
import pandas as pd

class _Sub_1(Submeasure):
    '''
    The Percentage of clients (12 years of age or older)
    with Major Depression or Dysthymia who reach Remission Six Months
    after an Index Event
    '''

    def __init__(self,engine:Engine,logger:Logger):
        super().__init__("DepRem")
        self.__CONN__ = engine
        self.__LOGGER__ = logger
        self.__index_visits__ = None

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
    def get_denominator(self)-> None:
        '''All clients aged 12 years or older with Major Depression or Dysthymia and an initial PHQ-9 or
        PHQ-9M score greater than nine on the Index Event Date'''
        self.__LOGGER__.info("Getting Denominator")
        try:
            super().get_denominator()
            self.__LOGGER__.info("Successfully got Denominator")
        except Exception:
            self.__LOGGER__.error('Failed to get Denominator',exc_info=True)
            raise

    
    @override
    def _get_populace(self) -> None:
        '''Gets all possible eligible clients for the denominator'''
        self.__initilize_populace()
        self.__get_index_visits()
        self.__set_index_groups()

    def __initilize_populace(self) -> None:
        '''Queries the database for starting populace'''
        # NOTE
        # if you want to make the entire code more dynamic and to be able to work for other companies and thier databases,
        # then this (and all SQL statements) should be replaced with factory methods
        # i.e. sql = DepRemSQLFactory.get_populace('PesachTikvah')
        sql = '''
            SELECT
                tblAssessmentToolsPHQA.PatientId AS PatientId,
                emr_PatientDetails.DOB,
                tblAssessmentToolsPHQA.EncounterID,
                tblEncounterSummary.VisitDateTime AS 'Date',
                CONVERT(INT,tblAssessmentToolsPHQA.TotalScore) AS 'Score' -- needed to match data type with the other Score column
            FROM
                tblAssessmentToolsPHQA -- stores PHQ9 data on adults
            LEFT JOIN
                tblEncounterSummary ON (tblAssessmentToolsPHQA.EncounterID = tblEncounterSummary.EncounterID)
            LEFT JOIN
                emr_PatientDetails ON (tblAssessmentToolsPHQA.PatientId = emr_PatientDetails.PatientId)
            WHERE
                tblEncounterSummary.VisitDateTime >= '2024-01-01'
            UNION -- joins the adult data on top of the chigin data
            SELECT
                tblAssessmentToolsPHQuetions.PatientId,
                emr_PatientDetails.DOB,
                tblAssessmentToolsPHQuetions.EncounterID,
                tblEncounterSummary.VisitDateTime AS 'Date',
                CONVERT(INT,tblAssessmentToolsPHQuetions.TotalScore) AS 'Score' -- db has these values as strings...
            FROM
                tblAssessmentToolsPHQuetions -- stores PHQ9 data on children
            LEFT JOIN
                tblEncounterSummary ON (tblAssessmentToolsPHQuetions.EncounterID = tblEncounterSummary.EncounterID)
            LEFT JOIN
                emr_PatientDetails ON (tblAssessmentToolsPHQuetions.PatientId = emr_PatientDetails.PatientId)
            WHERE
                tblEncounterSummary.VisitDateTime >= '2024-01-01'
            '''
        self.__populace__ = pd.read_sql(sql,self.__CONN__).sort_values(['PatientId','Date'])

    def __get_index_visits(self) -> None:
        '''Filters populace and finds the index visit for every patient'''
        # Index Event Date:
        # The date on which the first instance of elevated PHQ-9 or PHQ-9M greater than nine
        # AND diagnosis of Depression or Dysthymia occurs during the Measurement Year
        index_visits = self.__populace__[self.__populace__['Score'] > 9].copy() # get visits with scores greater than 9
        index_visits['measurement_year'] = index_visits['Date'].dt.year # create year field to track returning patients across multiple years
        index_visits['patient_measurement_year_id'] = index_visits['PatientId'].astype(str) + '-' + index_visits['measurement_year'].astype(str)
        index_visits = index_visits[['PatientId','patient_measurement_year_id','EncounterID','measurement_year','Date']] # keep needed data to track index visits
        index_visits = index_visits.sort_values('Date') # reorder visits by date
        index_visits = index_visits.drop_duplicates('patient_measurement_year_id',keep='first') # keep the first index visit per patient per year
        self.__index_visits__ = index_visits

    def __set_index_groups(self) -> None:
        '''Matches all encounters in populace to their corresponding index visit'''
        index_groups = self.__index_visits__.groupby('PatientId')
        self.__populace__[['patient_measurement_year_id','index_encounter_id']] = self.__populace__.apply(lambda visit: pd.Series(self.__create_patient_measurement_year_id(visit, index_groups)),axis=1)
        self.__populace__ = self.__populace__[self.__populace__['patient_measurement_year_id'] != 'No Group'] # removes encounters that didn't follow an index visit 

    def __create_patient_measurement_year_id(self,visit:pd.Series,index_groups:pd.DataFrame.groupby) -> str:
        '''Creates a unique id to match visits to their coresponding index visit \n
        Returns: patient_measurement_year_id'''
        # patient_measurement_year_id = 'PatientId' + '-' + 'Year of Index Visit(meausrement year)' (i.e 123456-2024)
        pid = visit['PatientId']
        if pid not in index_groups.groups: # short ciruits if this Patient ID never had an index visit
            return 'No Group'
        ind_group = index_groups.get_group(pid) # get every index visit for the current patient
        visit_date = visit['Date'].date()
        index_mask = [visit_date >= elem for elem in ind_group['Date'].dt.date] # find index dates that occured before the encounter date
        if index_mask.count(True) >= 1:
            index_visit= ind_group[index_mask].iloc[-1] # filter down to the last index date before the encounter date
            index_date = index_visit['Date']
            uid = str(pid) + '-' + str(index_date.year)
            index_encounter_id = index_visit['EncounterID']
            return(uid,index_encounter_id)
        else:
            return('No Group')


    @override
    def _remove_exclusions(self) -> None:
        '''Filters exclusions from populace'''
        exclusions = self.__get_all_exclusions()
        ranges = self.__determine_exclusion_date_range()
        exclusions = self.__compare_exclusions_with_range(exclusions, ranges)
        self.__filter_out_exclusions(exclusions)

    def __get_all_exclusions(self) -> pd.DataFrame:
        '''Queries the database for all visits that would exclude a patient from the denominator \n
        Returns: all_exclusions'''
        # NOTE
        # if you want to make the entire code more dynamic and to be able to work for other companies and thier databases,
        # then this (and all SQL statements) should be replaced with factory methods
        # i.e. sql = DepRemSQLFactory.get_exclusions('PesachTikvah')
        sql = '''
            SELECT
                Diagnosis.PatientId,
                CONVERT(DATE,tblEncounterSummary.VisitDateTime) AS 'Date' -- remove timestamp from datetime
            FROM
                Diagnosis
            LEFT JOIN
                tblEncounterSummary ON (Diagnosis.EncounterId = tblEncounterSummary.EncounterID)
            WHERE
                ICD10 LIKE 'F30.%' -- Bipolar
                OR
                ICD10 LIKE 'F31.%' -- Bipolar
                OR
                ICD10 = 'F34.0' -- Personality
                OR
                ICD10 = 'F60.3' -- Personality
                OR
                ICD10 = 'F60.4' -- Personality
                OR
                ICD10 LIKE 'F68.1%' -- Personality
                OR
                ICD10 LIKE 'F20.%' -- Schizophrenia/Pyschotic
                OR
                ICD10 = 'F21' -- Schizophrenia/Pyschotic
                OR
                ICD10 = 'F23' -- Schizophrenia/Pyschotic
                OR
                ICD10 LIKE 'F25.%' -- Schizophrenia/Pyschotic
                OR
                ICD10 = 'F28' -- Schizophrenia/Pyschotic
                OR
                ICD10 = 'F29' -- Schizophrenia/Pyschotic
                OR
                ICD10 = 'F84.0' -- Pervasive Development
                OR
                ICD10 = 'F84.3' -- Pervasive Development
                OR
                ICD10 = 'F84.8' -- Pervasive Development
                OR
                ICD10 = 'F84.9' -- Pervasive Development
                OR
                ICD10 = 'Z51.1' -- Palliative
        '''
        return pd.read_sql(sql,self.__CONN__).drop_duplicates()

    def __determine_exclusion_date_range(self) -> pd.DataFrame:
        '''Calculates the exclusion date range for all index groups \n
        Returns: ranges'''
        # an exclusion can occur any time prior to the end of a patient's numerator Measurement Period (index visit date + 6 months + 60 days)
        encounters = self.__index_visits__[['PatientId','patient_measurement_year_id','EncounterID','Date']].copy()
        encounters['end_exclusion_range'] = encounters.apply(lambda visit: datetime(
                                                                                (visit['Date'] + pd.DateOffset(months=6) + pd.DateOffset(days=60)).year,
                                                                                (visit['Date'] + pd.DateOffset(months=6) + pd.DateOffset(days=60)).month,
                                                                                (visit['Date'] + pd.DateOffset(months=6) + pd.DateOffset(days=60)).day
                                                                            ).date() ,axis=1)
        return(encounters)

    def __compare_exclusions_with_range(self, exclusions:pd.DataFrame, ranges:pd.DataFrame) -> list:
        '''Filters out all exclusions that aren't within their index_group exclusion range \n
        Params: exclusions, ranges \n
        Returns: valid_exclusions'''
        # a visit is only excluded if the exclusion happened durring the index visit's remission year/range
        # therefore it's needed to filter the exclusions to only the ones that occured durring that period
        exclusions.rename(columns={'Date':'exclusion_date'},inplace=True)
        ranges.rename(columns={'Date':'index_date'},inplace=True)
        exclusions = exclusions.merge(ranges, how ='left', on='PatientId') # merge the exclusions on top of the visits in order to compare dates
                                                                           # Can not use patient_measurement_year_id because it doesn't contain the full date
                                                                           # so it will exclude a false negative if an index visit occured at the end of
                                                                           # a year and the exclusion happens early the next year
        exclusions['valid_exclusion'] = exclusions['exclusion_date'] <= exclusions['end_exclusion_range'] # check if the exclusion date occured before the exclusion range
        exclusions = exclusions[exclusions['valid_exclusion']] # keep only the exclusions durring that range
        return exclusions['EncounterID'].drop_duplicates().to_list()

    def __filter_out_exclusions(self, exclusions) -> None:
        '''Filters out all index groups from populace that have a valid exclusion \n
        Params: exclusions'''
        self.__populace__['exclusion'] = self.__populace__['index_encounter_id'].isin(exclusions) # check if the patient_measurement_year_id is in the exclusion list
        self.__populace__ = self.__populace__[~self.__populace__['exclusion']].copy() # filter out all invalid visits


    @override
    def get_numerator(self) -> None:
        '''All clients in the denominator who achieved Remission at Six Months as demonstrated by a
        Six Month (+/- 60 days) PHQ-9 or PHQ-9M score of less than five'''
        self.__LOGGER__.info('Getting Numerator')
        try:
            super().get_numerator()
            self.__LOGGER__.info("Successfully got Numerator")
        except Exception:
            self.__LOGGER__.error('Failed to get Numerator',exc_info=True)
            raise


    @override
    def _apply_time_constraint(self) -> None:
        '''Filters out all visits that aren't within the index visit's frequency +/- range'''
        frequency = relativedelta(months=6)
        range = relativedelta(days=60)
        groups = self.__populace__.groupby('patient_measurement_year_id')
        results = pd.DataFrame() #create an empty insurance_data to store concatenate important visits to
        for name, patient_data in groups:
            index_visit = patient_data.iloc[0] # the index visit is stored at [0] because self.__populace__ is sortedby date and all visits before an idex visit were removed by self.__set_index_groups()
            enc_date = index_visit['Date']
            enc_date = pd.to_datetime(enc_date)
            min_range = enc_date + frequency - range # find the earliest possible remission date
            max_range = enc_date + frequency + range # find the latest possible remission date
            patient_data = patient_data[(patient_data['Date'] >= min_range) & (patient_data['Date'] <= max_range)] # remove all visits that aren't within the remission range
            results = pd.concat([results, index_visit.to_frame().T], ignore_index=True) # add the index visit to results
            results = pd.concat([results, patient_data], ignore_index=True) # add the visits within the remission range to results
        self.__populace__ = results

    @override
    def _find_performance_met(self) -> None:
        '''Finds all valid remissions index groups in populace'''
        numerator_ids = self.__check_numerator_condition()
        self.__set_numerator(numerator_ids)

    def __check_numerator_condition(self) -> list:
        '''Checks all index groups to find which ones are valid numerators \n
        Returns: remission_ids'''
        # Remission: A PHQ-9 or PHQ-9M score of less than five
        groups = self.__populace__.groupby('patient_measurement_year_id')
        remission_ids = [] # create a list for all valid remission ids to be added to
        for name, index_group in groups:
            if True in (index_group['Score'] < 5).to_list():
                remission_ids.append(name)
        return remission_ids
    
    def __set_numerator(self, numerator_ids:list) -> None:
        '''Flags all index groups if they are valid for the numerator or not'''
        self.__populace__['numerator'] = self.__populace__['patient_measurement_year_id'].isin(numerator_ids)



    @override
    def stratify_data(self) -> None:
        '''Gets stratification for all clients: age, medicaid, ethnicity and race'''
        self.__LOGGER__.info('Getting Stratification')
        try:
            self.__initialize_stratify()
            self.__calculate_age()
            self.__get_stratify_from_db()
            self.__fill_blank_stratify()
            self.__LOGGER__.info("Successfully got Stratification")
        except Exception:
            self.__LOGGER__.error('Failed to get Stratification',exc_info=True)
            raise

    def __initialize_stratify(self) -> None:
        '''Initializes self.__stratify__ by filtering self.__populace__'''
        # use populace to initialize stratify instead of index_visits because populace is filtered and index_visits still has exclusions in it
        self.__stratify__ = self.__populace__[['patient_measurement_year_id','PatientId','DOB', 'index_encounter_id','Date']].sort_values(['index_encounter_id','Date']).drop_duplicates('index_encounter_id')

    def __calculate_age(self) -> None:
        '''Calculates age stratification at the time of index visit'''
        self.__stratify__['Age'] = (self.__stratify__['Date'] - self.__stratify__['DOB']).apply(lambda val: val.days//365.25) # calculates age at visit
        self.__stratify__ = self.__stratify__[self.__stratify__['Age'] >= 12] # filters out ages less than 12
        self.__stratify__['Age'] = self.__stratify__['Age'].apply(lambda age: '18+' if age >= 18 else '12-18') # sets age index_group

    def __get_stratify_from_db(self) -> None:
        '''Gets remaining stratification data from the database (race, ethnicity, medicaid)'''
        self.__get_patient_data()
        self.__get_encounter_data()

    def __get_patient_data(self) -> None:
        '''Gets patient stratifications from database (ethnitity and race)'''
        # NOTE
        # if you want to make the entire code more dynamic and to be able to work for other companies and thier databases,
        # then this (and all SQL statements) should be replaced with factory methods
        # i.e. sql = DepRemSQLFactory.get_patient_data('PesachTikvah')
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
        patient_data = pd.read_sql(sql,self.__CONN__).drop_duplicates('PatientId',keep='last')
        self.__stratify__ = self.__stratify__.merge(patient_data)

    def __get_encounter_data(self) -> None:
        '''Gets encounter stratification (medicaid)'''
        medicaid_data = self.__get_medicaid_from_db()
        medicaid_data = self.__merge_mediciad_with_stratify(medicaid_data)
        medicaid_data = self.__filter_insurance_dates(medicaid_data)
        medicaid_data['patient_measurement_year_id'] = self.__recreate_patient_measurement_year_id(medicaid_data)
        results = self.__determine_medicaid_stratify(medicaid_data)
        self.__stratify__ = self.__stratify__.merge(results)

    def __get_medicaid_from_db(self) -> pd.DataFrame:
        '''Queries data base for all relevant patients' insurance information'''
        # NOTE
        # if you want to make the entire code more dynamic and to be able to work for other companies and thier databases,
        # then this (and all SQL statements) should be replaced with factory methods
        # i.e. sql = DepRemSQLFactory.get_medicaid('PesachTikvah')
        sql = f'''
            Select
                PatientId,
                EffectiveDate AS 'Start',
                DisenrollmentDate AS 'End',
                LOWER ([tblPayerPlans.PayerPlanName]) AS 'Plan'
            FROM
                tblPatientPayers
            WHERE
                PatientId IN {tuple(self.__stratify__['PatientId'].tolist())} -- creates a list of valid PatientIds
            '''
        return pd.read_sql(sql,self.__CONN__)

    def __merge_mediciad_with_stratify(self,medicaid_data:pd.DataFrame) -> pd.DataFrame:
        '''Merges stratify data on top of the medicaid data \n
        Returns: merged_data'''
        return medicaid_data.merge(self.__stratify__[['PatientId','Date']])

    def __filter_insurance_dates(self,medicaid_data:pd.DataFrame) -> pd.Series:
        '''Removes insurances that weren't active at the time of the patient's visit \n
        Returns: valid_medicaid'''
        medicaid_data['End'] = medicaid_data['End'].fillna(datetime.now()) # replace nulls with today so that they don't get filtered out
        medicaid_data['valid'] = (medicaid_data['Start'] <= medicaid_data['Date']) & (medicaid_data['End'] >= medicaid_data['Date']) # checks if the insurance is valid at time of encounter
        return medicaid_data[medicaid_data['valid']].copy()

    def __recreate_patient_measurement_year_id(self,medicaid_data:pd.DataFrame) -> pd.Series:
        '''creates the patient measurement year for compatibility with the populace \n
        Returns: patient_measurement_year_id'''
        return (medicaid_data['PatientId'].astype(str) + '-' + medicaid_data['Date'].astype(str)).apply(lambda val: val[:11])

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
        medicaid_data = medicaid_data.merge(self.__stratify__[['PatientId','Date','index_encounter_id']],on=['PatientId','Date'])
        return (medicaid_data.groupby(['index_encounter_id'])['Medicaid'].sum() == 1).reset_index()

    def __fill_blank_stratify(self) -> None:
        '''Fill in all null values with Unknown'''
        self.__stratify__ = self.__stratify__.fillna('Unknown')

    

    @override
    def return_final_data(self) -> tuple[pd.DataFrame,pd.DataFrame]:
        '''Returns the final calculated data for the DEP-REM measurement \n
        Returns: \n
            tuple[pd.DataFrame, pd.DataFrame]: \n
                - The first DataFrame represents the processed populace data
                - The second DataFrame represents the processed stratified data'''
        self.__trim_unnecessary_populace_data()
        self.__fix_populace_data_types()
        self.__trim_unnecessary_stratify_data()
        return self.__populace__.copy(), self.__stratify__.copy()


    def __trim_unnecessary_populace_data(self) -> None:
        '''Gets rid of all data that isn't needed to calculate the denominator numerator'''
        self.__populace__ = self.__populace__[['PatientId','patient_measurement_year_id','index_encounter_id','numerator']].drop_duplicates() 

    def __fix_populace_data_types(self) -> None:
        '''Converts the columns to their propper datatype for sql'''
        self.__populace__['PatientId'] = self.__populace__['PatientId'].astype(int) # there was some err when pushing to DB... so the casting data types fixes the issue, not sure what caused the err
        self.__populace__['patient_measurement_year_id'] = self.__populace__['patient_measurement_year_id'].astype(str)
        self.__populace__['index_encounter_id'] = self.__populace__['index_encounter_id'].astype(int)
        self.__populace__['numerator'] = self.__populace__['numerator'].astype(bool)

    def __trim_unnecessary_stratify_data(self) -> None:
        '''Gets rid of all data that isn't needed to stratify the denominator numerator'''
        self.__stratify__ = self.__stratify__[['patient_measurement_year_id','Age','Ethnicity','Race','Medicaid']]




class DEP_REM(Measurement):
    """
    The DEP-REM-6 measure calculates the Percentage of clients (12 years of age or older) with
    Major Depression or Dysthymia who reach Remission Six Months (+/- 60 days) after an Index
    Event Date
    """

    def __init__(self,engine:Engine,logger:Logger):
        super().__init__("DEP REM")
        self.__sub1__:Submeasure = _Sub_1(engine,logger)
    
    @override
    def get_submeasure_data(self) -> dict[str,pd.DataFrame]:
        """
        Calculates all the data for the DEP REM 6 Measurement and its Submeasures

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
 