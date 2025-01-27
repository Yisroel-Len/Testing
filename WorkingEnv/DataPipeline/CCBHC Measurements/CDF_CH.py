from typing import override
from sqlalchemy import Engine
from datetime import datetime
from logging import Logger
from Measurement import Measurement, Submeasure
import pandas as pd

class _Sub_1 (Submeasure):
    '''
    Percentage of minors screened for depression during the measurement year
    using an age-appropriate standardized depression screening tool,
    and if positive, a follow-up plan is documented on the date of the eligible encounter
    '''

    def __init__(self,engine:Engine,logger:Logger):
        super().__init__("CDF_CH")
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
        '''All clients with an outpatient visit during the measurement year'''
        self.__LOGGER__.info("Sub 1 Getting Denominator")
        try:
            super().get_denominator()
            self.__LOGGER__.info("Sub 1 Successfully got Denominator")
        except Exception:
            self.__LOGGER__.error("Sub 1 Failed to get Denominator",exc_info=True)
            raise

    @override
    def _get_populace(self) -> None:
        '''Gets all possible eligible clients for the denominator'''
        self.__initialize_populace()
        self.__populace__['patient_measurement_year_id'] = self.__create_measurement_year_id(self.__populace__['PatientId'],self.__populace__['VisitDateTime'])
        self.__populace__ = self.__populace__.sort_values(by=['patient_measurement_year_id','VisitDateTime']).drop_duplicates('patient_measurement_year_id',keep='first')

    def __initialize_populace(self) -> None:
        '''Queries db for all clients seen during the Measurement Year'''
        sql = '''
        SELECT
            tblEncounterSummary.PatientId,
            tblEncounterSummary.EncounterId,
            tblEncounterSummary.VisitDateTime,
            emr_PatientDetails.DOB
        FROM
            tblEncounterSummary
        INNER JOIN
                ptPatient_Activity ON (tblEncounterSummary.PatientId = ptPatient_Activity.PatientID)
        LEFT JOIN
            emr_PatientDetails ON (tblEncounterSummary.PatientId = emr_PatientDetails.PatientId)
        LEFT JOIN
            tblEncounterTypeCPTMap ON (tblEncounterSummary.EncounterTypeId = tblEncounterTypeCPTMap.EncounterTypeID)
        WHERE
            YEAR(tblEncounterSummary.VisitDateTime) >= 2024
            AND
            tblEncounterTypeCPTMap.CPTCode IN ('59400', '59510', '59610', '59618', '90791', '90792', '90832',
                                               '90834', '90837', '92625', '96105', '96110', '96112', '96116',
                                               '96125', '96136', '96138', '96156', '96158', '97161', '97162',
                                               '97163', '97165', '97166', '97167', '99078', '99202', '99203',
                                               '99204', '99205', '99212', '99213', '99214', '99215', '99304',
                                               '99305', '99306', '99307', '99308', '99309', '99310', '99315',
                                               '99316', '99318', '99324', '99325', '99326', '99327', '99328',
                                               '99334', '99335', '99336', '99337', '99339', '99340', '99401',
                                               '99402', '99403', '99483', '99484', '99492', '99493', '99384',
                                               '99385', '99386', '99387', '99394', '99395', '99396', '99397')
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
        # All clients who have been diagnosed with depression or bipolar disorder
        self.__remove_age_exclusion()
        self.__remove_mental_exclusions()

    def __remove_age_exclusion(self) -> None:
        '''Finds and reomves all clients aged 17 years or younger'''
        self.__calculate_age()
        self.__filter_age()

    def __calculate_age(self) -> None:
        '''Calculates age of client at the date of service'''
        self.__populace__['age'] = (self.__populace__['VisitDateTime'] - self.__populace__['DOB']).dt.days // 365.25

    def __filter_age(self) -> None:
        '''Removes all clients aged 17 or younger at the date of service'''
        self.__populace__ = self.__populace__[(self.__populace__['age'] >= 12) & (self.__populace__['age'] <= 17)]

    def __remove_mental_exclusions(self) -> None:
        '''Finds and removes all patients with a diagnosis of depression or bipolar prior to their measurement year'''
        # The ICD 10 codes for depressions and bipolars are given on p.75 of the following pdf
        # https://www.samhsa.gov/sites/default/files/ccbhc-quality-measures-technical-specifications-manual.pdf
        d = self.__get_depressions()
        self.__filter_mental_exclsusion(d)
        b = self.__get_bipolars()
        self.__filter_mental_exclsusion(b)

    def __get_depressions(self) -> pd.DataFrame:
        '''Finds all patients with a diagnosis of depression \n
        Returns: depressions'''
        sql = '''
        SELECT
            Diagnosis.PatientId,
            tblEncounterSummary.VisitDateTime AS 'exclusion_date'
        FROM
            Diagnosis
        LEFT JOIN
            tblEncounterSummary ON (Diagnosis.EncounterId = tblEncounterSummary.EncounterId)
        INNER JOIN
                ptPatient_Activity ON (Diagnosis.PatientId = ptPatient_Activity.PatientID)
        WHERE
            ICD10 IN ('F01.51',
                      'F32.A','F32.0','F32.1','F32.2','F32.3','F32.4','F32.5','F32.89','F32.9',
                      'F33.0','F33.1','F33.2','F33.3','F33.40','F33.41','F33.42','F33.8','F33.9',
                      'F34.1','F34.81','F34.89',
                      'F43.21','F43.23',
                      'F53.0','F53.1',
                      'O90.6',
                      'O99.340','O99.341','O99.342','O99.343','O99.345')
        '''
        return pd.read_sql(sql,self.__CONN__).drop_duplicates()

    def __get_bipolars(self) -> pd.DataFrame:
        '''Finds all patients with a diagnosis of bipolar \n
        Returns: bipolar'''
        sql = '''
        SELECT
            Diagnosis.PatientId,
            tblEncounterSummary.VisitDateTime AS 'exclusion_date'
        FROM
            Diagnosis
        LEFT JOIN
            tblEncounterSummary ON (Diagnosis.EncounterId = tblEncounterSummary.EncounterId)
        INNER JOIN
            ptPatient_Activity ON (Diagnosis.PatientId = ptPatient_Activity.PatientID)
        WHERE
            ICD10 IN ('F31.10','F31.11','F31.12','F31.13',
                      'F31.2',
                      'F31.30','F31.31','F31.32',
                      'F31.4',
                      'F31.5',
                      'F31.60','F31.61','F31.62','F31.63','F31.64',
                      'F31.70','F31.71','F31.72','F31.73','F31.74','F31.75','F31.76','F31.77','F31.78',
                      'F31.81','F31.89',
                      'F31.9')
        ORDER BY
            Diagnosis.PatientId
        '''
        return pd.read_sql(sql,self.__CONN__).drop_duplicates()

    def __filter_mental_exclsusion(self, exclusions:pd.DataFrame) -> None:
        '''Removes all patients with an exclusion diagnosis prior to their measurement year'''
        exclusions = exclusions.sort_values(by=['PatientId','exclusion_date']).drop_duplicates('PatientId',keep='first')
        exclusions['exclusion_date'] = pd.to_datetime(exclusions['exclusion_date'])
        self.__populace__ = self.__populace__.merge(exclusions,how='left')
        self.__populace__ = self.__populace__[(self.__populace__['VisitDateTime'] <= self.__populace__['exclusion_date']) | self.__populace__['exclusion_date'].isna()]
        self.__populace__ = self.__populace__.drop(columns='exclusion_date')



    @override
    def get_numerator(self) -> None:
        '''[Clients] screened for depression on the date of the encounter or 14 days prior to the
        date of the encounter using an age-appropriate standardized depression screening tool AND, if
        positive, a follow-up plan is documented on the date of the eligible encounter'''
        # NOTE the screening date has been since updated to being required once per measurement year, independent of an encounter date
        # https://www.samhsa.gov/sites/default/files/ccbhc-quality-measures-faq.pdf see p. 22 "At which encounters would screening need to occur?"
        self.__LOGGER__.info("Sub 1 Getting Numerator")
        try:
            super().get_numerator()
            self.__LOGGER__.info("Sub 1 Successfully Got Numerator")
        except Exception:
            self.__LOGGER__.error("Sub 1 Failed to get Numerator",exc_info=True)
            raise
        
    @override
    def _find_performance_met(self) -> None:
        '''Finds clients with negative screenings or positive screenings with follow ups'''
        self.__add_screenings_to_populace()
        self.__determine_screenings_results()
        # split populace in order to add a ['numerator_desc'] which is different for positive/negative screening results
        # this is done to avoid df.apply and instead setting an entire column value at once O(1) instead of an iteration O(n)
        negative_screenings = self.__populace__[~self.__populace__['positive_screening']].copy()
        negative_screenings = self.__set_negative_numerators(negative_screenings)
        positive_screenings = self.__populace__[self.__populace__['positive_screening']].copy()
        positive_screenings = self.__set_positive_numerators(positive_screenings)
        self.__populace__ = pd.concat([positive_screenings,negative_screenings])

    def __add_screenings_to_populace(self) -> None:
        '''Queries db for all screening results and adds them to populace'''
        screenings = self.__get_screenings()
        screenings = self.__prep_screenings_for_merge(screenings)
        self.__populace__ = self.__populace__.merge(screenings,on='patient_measurement_year_id',how='left')

    def __get_screenings(self) -> pd.DataFrame:
        '''Queries db for all clients screening \n
        Returns: screening_results'''
        # A normalized and validated depression screening tool developed for the population in which it is being utilized.
        # Examples of depression screening tools include but are not limited to:
        # Patient Health Questionnaire for Adolescents (PHQ-A),
        # Beck Depression Inventory-Primary Care Version (BDI-PC),
        # Mood Feeling Questionnaire (MFQ),
        # Center for Epidemiologic Studies Depression Scale (CES-D),
        # Patient Health Questionnaire (PHQ-9),
        # Pediatric Symptom Checklist (PSC-17),
        # and PRIME MD-PHQ2

        # I use the PHQ-A because it is also used in the DEP-REM-6
        sql = '''
        SELECT
            tblAssessmentToolsPHQA.PatientId,
            tblAssessmentToolsPHQA.EncounterID,
            tblEncounterSummary.VisitDateTime AS 'screening_date',
            tblAssessmentToolsPHQA.TotalScore
        FROM 
            tblAssessmentToolsPHQA
        INNER JOIN
            ptPatient_Activity ON (tblAssessmentToolsPHQA.PatientId = ptPatient_Activity.PatientID)
        LEFT JOIN
            tblEncounterSummary ON (tblAssessmentToolsPHQA.EncounterID = tblEncounterSummary.EncounterID)
        '''
        return pd.read_sql(sql,self.__CONN__)

    def __prep_screenings_for_merge(self,screenings:pd.DataFrame) -> pd.DataFrame:
        '''Fixes up screenings so that it can be merged into populace
        Parameters: screenings \n
        Returns: screening'''
        screenings['patient_measurement_year_id'] = self.__create_measurement_year_id(screenings['PatientId'],screenings['screening_date'])
        # "The measure assesses the most recent depression screening completed either during the eligible encounter or within the 14 days prior to that encounter"
        screenings = screenings.rename(columns={'EncounterID':'screening_encounter_id',
                                                'TotalScore':'screening_score'})
        screenings = screenings.sort_values(['patient_measurement_year_id','screening_date']).drop_duplicates(keep='last')
        screenings = screenings.drop(columns={'PatientId'})
        screenings['screening_score'] = pd.to_numeric(screenings['screening_score'],errors='coerce')
        return screenings

    def __determine_screenings_results(self) -> None:
        '''Creates a column showing if clients scored positive or negative on thier screening'''
        # Having a score of 9- does not require a follow up plan
        # https://www.hiv.uw.edu/page/mental-health-screening/phq-9
        self.__populace__['positive_screening'] = self.__populace__['screening_score'] > 9

    def __set_negative_numerators(self, negative_screenings:pd.DataFrame) -> pd.DataFrame:
        '''Adds numerator fields for patients with negative screening results \n
        Returns: negative_screenings'''
        negative_screenings['numerator'] = True
        negative_screenings['numerator_desc'] = 'Negative screening'
        return negative_screenings

    def __set_positive_numerators(self, positive_screenings:pd.DataFrame) -> pd.DataFrame:
        '''Adds numerator fields for patients with positive screening results \n
        Returns: positive_screenings'''
        follow_ups = self.__find_follow_ups()
        positive_screenings = positive_screenings.merge(follow_ups,how='left')
        positive_screenings['numerator'] = (positive_screenings['last_encounter'] > positive_screenings['screening_date']) & (positive_screenings['last_encounter'].notna())
        # split positive_screenings so that the numerator fields only get set on clients who fulfilled numerator criteria
        numerator = positive_screenings[positive_screenings['numerator']].copy()
        numerator = self.__set_positive_numerator_fields(numerator)
        return pd.concat([numerator,positive_screenings[~positive_screenings['numerator']]])

    def __find_follow_ups(self) -> pd.DataFrame:
        '''Finds the most recent encounter for clients with a positve screening \n
        Returns: last_encounters'''
        # Referral to a provider for additional evaluation.
        # Pharmacological interventions.
        # Other interventions for the treatment of depression.
        last_encounters = self.__populace__.groupby('PatientId')['VisitDateTime'].max().to_frame().reset_index()
        last_encounters = last_encounters.rename(columns={'VisitDateTime':'last_encounter'})
        return last_encounters

    def __set_positive_numerator_fields(self,numerator:pd.DataFrame) -> pd.DataFrame:
        '''Sets numerator fields for patients with positive screening results and a follow up \n
        Parameters: positive_numerator
        Returns: positive_numerator'''
        numerator['numerator'] = True
        numerator['numerator_desc'] = 'Positive screening with follow up'
        return numerator


    @override
    def _apply_time_constraint(self) -> None:
        '''Checks to see if the follow up happened after the screening'''
        # NOTE this is not needed, as counseling should happen in the same session as the screening
        # which is checked in __set_positive_numerator by last_encounter > screening_date
        pass


    @override
    def stratify_data(self) -> None:
        '''Gets stratification for all clients: medicaid, ethnicity and race'''
        self.__LOGGER__.info("Sub 1 Getting Stratification")
        try:
            self.__initialize_stratify()
            self.__get_stratify_from_db()
            self.__fill_blank_stratify()
            self.__LOGGER__.info("Sub 1 Successfully got Stratification")
        except Exception:
            self.__LOGGER__.error("Sub 1 Failed to get Stratification",exc_info=True)
            raise

    def __initialize_stratify(self) -> None:
        '''Initializes self.__stratify__ by filtering self.__populace__'''
        self.__stratify__ = self.__populace__[['patient_measurement_year_id','PatientId','EncounterId','VisitDateTime','screening_date','last_encounter']].sort_values(['patient_measurement_year_id','EncounterId']).drop_duplicates('patient_measurement_year_id')
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
        patient_data = pd.read_sql(sql,self.__CONN__)
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

    def __filter_insurance_dates(self,medicaid_data:pd.DataFrame) -> pd.Series:
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
        '''Returns the final calculated data for the CDF-CH measurement \n
        Returns: \n
            tuple[pd.DataFrame, pd.DataFrame]: \n
                - The first DataFrame represents the processed populace data
                - The second DataFrame represents the processed stratified data'''
        self.__trim_unnecessary_populace_data()
        self.__trim_unnecessary_stratify_data()
        return self.__populace__.copy(), self.__stratify__.copy()
    
    def __trim_unnecessary_populace_data(self) -> None:
        '''Gets rid of all data that isn't needed to calculate the populace'''
        self.__populace__ = self.__populace__[['patient_measurement_year_id','PatientId','EncounterId','screening_encounter_id','last_encounter','numerator','numerator_desc']].drop_duplicates(subset='patient_measurement_year_id')


    def __trim_unnecessary_stratify_data(self) -> None:
        '''Gets rid of all data that isn't needed to stratify the populace'''
        self.__stratify__ = self.__stratify__[['patient_measurement_year_id','measurement_year','Ethnicity','Race','Medicaid']].drop_duplicates(subset='patient_measurement_year_id')


    

class CDF_CH(Measurement):
    """
    Percentage of beneficiaries [clients] ages 12 to 17 screened for depression on the date of the
    encounter or 14 days prior to the date of the encounter using an age-appropriate standardized
    depression screening tool, and if positive, a follow-up plan is documented on the date of the eligible
    encounter.
    """

    def __init__(self,engine:Engine,logger:Logger):
        super().__init__("CDF CH")
        self.__sub1__:Submeasure = _Sub_1(engine,logger)

    @override
    def get_submeasure_data(self) -> dict[str,pd.DataFrame]:
        """
        Calculates all the data for the CDF CH Measurement and its Submeasures

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