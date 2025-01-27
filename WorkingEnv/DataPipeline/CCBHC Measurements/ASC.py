from typing import override
from sqlalchemy import Engine
from datetime import datetime
from dateutil.relativedelta import relativedelta
from logging import Logger
from Measurement import Measurement, Submeasure
import pandas as pd

class _Sub_1 (Submeasure):
    '''Percentage of clients aged 18 years and older who were screened for unhealthy alcohol use
    using a Systematic Screening Method at least once within the last 12 months'''

    def __init__(self,engine:Engine,logger:Logger):
        super().__init__("ASC_sub_1")
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
        '''All clients aged 18 years and older seen for at least two visits or at least one preventive visit
        during the Measurement Year'''
        self.__LOGGER__.info("Sub 1 Getting Denominator")
        try:
            super().get_denominator()
            self.__LOGGER__.info("Sub 1 Successfully got Denominator")
        except Exception:
            self.__LOGGER__.error("Sub 1 Failed to get Denominator",exc_info=True)
            raise

    @override
    def _get_populace(self) -> None:
        '''Queries db for all eligible clients'''
        self.__get_two_or_more_visits()
        self.__get_preventive_visits()

    def __get_two_or_more_visits(self) -> None:
        '''Gets all patients who've had 2 or more visits per measurement year'''
        self.__get_all_encounters()
        self.__create_patient_measurement_year_id()
        self.__number_of_visits_filter()


    def __get_all_encounters(self) -> None:
        '''Queries db for all encounters over the measurement year'''
        sql = '''
        SELECT
            tblEncounterSummary.PatientId,
            DATEDIFF(year,emr_PatientDetails.DOB,VisitDateTime) AS 'Age',
            tblEncounterSummary.EncounterID,
            VisitDateTime AS 'encounter_date',
            YEAR(VisitDateTime) AS 'measurement_year'
        FROM
            tblEncounterSummary
        LEFT JOIN
            emr_PatientDetails ON (tblEncounterSummary.PatientId = emr_PatientDetails.PatientId)
        INNER JOIN
            ptPatient_Activity ON (tblEncounterSummary.PatientId = ptPatient_Activity.PatientID)
        WHERE
            VisitDateTime >= '2024-01-01'
        ORDER BY
            tblEncounterSummary.PatientId,
            measurement_year
        '''
        self.__populace__ = pd.read_sql(sql,self.__CONN__)

    def __create_patient_measurement_year_id(self) -> None:
        '''Creates patient_measurement_year_id'''
        self.__populace__['patient_measurement_year_id'] = self.__populace__['PatientId'].astype(str) + '-' + self.__populace__['measurement_year'].astype(str)

    def __number_of_visits_filter(self) -> None:
        '''Removes clients who've only had 1 visit per measurement year'''
        multiple_visits = (self.__populace__.groupby('patient_measurement_year_id')['PatientId'].size() >= 2).reset_index()['patient_measurement_year_id'].to_list()
        self.__populace__ = self.__populace__[self.__populace__['patient_measurement_year_id'].isin(multiple_visits)].copy()

    def __get_preventive_visits(self) -> None:
        '''Gets all patients who've had preventive visits during the measurement year
        and adds them to populace'''
        preventive_visits = self.__get_preventive_visits_from_db()
        if preventive_visits.empty: # break out early from __get_preventive_visits() if there are no preventive visits
            return
        preventive_visits['patient_measurement_year_id'] = self.__create_patient_measurement_year_id_for_preventive_visits(preventive_visits)
        self.__add_preventive_visits_to_populace(preventive_visits)

    def __get_preventive_visits_from_db(self) -> pd.DataFrame:
        '''Queries db for all preventive visits over the measurement year \n
        Returns: preventive_visits'''
        # preventive CPT Codes can be found at https://www.samhsa.gov/sites/default/files/ccbhc-quality-measures-technical-specifications-manual.pdf, page 54/55
        sql = '''
        SELECT
            tblEncounterSummary.PatientId,
            DATEDIFF(year,emr_PatientDetails.DOB,VisitDateTime) AS 'Age',
            --emr_PatientDetails.DOB,
            tblEncounterSummary.EncounterID,
            VisitDateTime AS 'encounter_date',
            YEAR(VisitDateTime) AS 'measurement_year'
        FROM
            tblEncounterSummary
        LEFT JOIN
            emr_PatientDetails ON (tblEncounterSummary.PatientId = emr_PatientDetails.PatientId)
        LEFT JOIN
            tblEncounterTypeCPTMap ON (tblEncounterSummary.EncounterTypeId = tblEncounterTypeCPTMap.EncounterTypeID)
        INNER JOIN
            ptPatient_Activity ON (tblEncounterSummary.PatientId = ptPatient_Activity.PatientID)
        WHERE
            CPTCode IN ('99385', '99386', '99387', '99395', '99396', '99397', '99401', '99402',
                        '99403', '99404', '99411', '99412', '99429', 'G0438', 'G0439')
        '''
        return pd.read_sql(sql,self.__CONN__)

    def __create_patient_measurement_year_id_for_preventive_visits(self, preventive_visits) -> pd.Series:
        '''Creates patient_measurement_year_id for preventive visits \n
        Returns: patient_measurement_year_id'''
        return preventive_visits['PatientId'].astype(str) + '-' + preventive_visits['measurement_year'].astype(str)

    def __add_preventive_visits_to_populace(self, preventive_visits) -> None:
        '''Adds preventive visits to populace'''
        self.__populace__ = pd.concat(self.__populace__,preventive_visits)

    @override
    def _remove_exclusions(self) -> None:
        '''Filters exclusions from populace'''
        # Denominator Exclusions:
        # All clients aged 17 years or younger
        # OR Clients with dementia at any time during the patient’s history through the end of the Measurement Year
        # OR Clients who use hospice services any time during the Measurement Year
        self.__remove_age_exclusions()
        self.__remove_dementia_exclusions()
        # self.__remove_hospice_exclusions() # NOTE This is something we don't track, therefore it is commented out. but the code for it should be very similar to the dementia code with a few changes in sql and comparing dates

    def __remove_age_exclusions(self) -> None:
        '''Removes all clients who are under 18'''
        self.__populace__ = self.__populace__[self.__populace__['Age'] >= 18].copy()

    def __remove_dementia_exclusions(self) -> None:
        '''Finds and removes all clients who have had dementia'''
        # Clients with dementia at any time during the patient’s history
        # through the end of the Measurement Year
        dementia = self.__get_dementia_exclusions()
        dementia['patient_measurement_year_id'] = self.__create_dementia_patient_measurement_year_id(dementia)
        exclusion_ids = self.__compare_dementia_year_to_populace(dementia)
        self.__filter_dementia_exclusions(exclusion_ids)

    def __get_dementia_exclusions(self) -> pd.DataFrame:
        '''Queries db for all clients who've had dementia \n
        Returns: dementia_clients'''
        sql = '''
        SELECT
            Diagnosis.PatientId,
            YEAR(tblEncounterSummary.VisitDateTime) AS 'exclusion_year'
        FROM
            Diagnosis
        LEFT JOIN
            tblEncounterSummary ON (Diagnosis.EncounterId = tblEncounterSummary.EncounterID)
        WHERE
            (
            ICD10 LIKE 'F01.%' -- Vascular dementia
            OR
            ICD10 LIKE 'F02.%' -- Dementia in other diseases classified elsewhere
            OR
            ICD10 LIKE 'F03.%' -- Unspecified dementia
            )
        '''
        return pd.read_sql(sql,self.__CONN__)

    def __create_dementia_patient_measurement_year_id(self, dementia) -> pd.Series:
        '''Creates patient_measurement_year_id for dementia clients \n
        Returns: patient_measurement_year_id'''
        return dementia['PatientId'].astype(str) + '-' + dementia['exclusion_year'].astype(str)

    def __compare_dementia_year_to_populace(self, dementia) -> list:
        '''Finds clients who've had dementia prior to the end of their measurement year \n
        Returns: exclusion_ids'''
        unique_denominators = self.__populace__[['PatientId','measurement_year','patient_measurement_year_id']].drop_duplicates('patient_measurement_year_id')
        unique_denominators = unique_denominators.merge(dementia[['PatientId','exclusion_year']],how='left',left_on='PatientId',right_on='PatientId')
        unique_denominators['to_exclude'] = unique_denominators['measurement_year'] >= unique_denominators['exclusion_year']
        exclusion_ids = unique_denominators[unique_denominators['to_exclude']]['patient_measurement_year_id'].drop_duplicates().to_list()
        return exclusion_ids

    def __filter_dementia_exclusions(self, exclusion_ids) -> None:
        '''Removes clients who've had dementia'''
        self.__populace__ = self.__populace__[~self.__populace__['patient_measurement_year_id'].isin(exclusion_ids)].copy()


    @override
    def get_numerator(self) -> None:
        '''All clients in the denominator who were screened for unhealthy alcohol use using a Systematic
        Screening Method at least once within the last 12 months'''
        self.__LOGGER__.info("Sub 1 Getting Numerator")
        try:
            super().get_numerator()
            self.__LOGGER__.info("Sub 1 Successfully got Numerator")
        except Exception:
            self.__LOGGER__.error("Sub 1 Failed to get Numerator",exc_info=True)
            raise

    @override
    def _apply_time_constraint(self) -> None:
        '''Finds the most recent encounter per client per measurement year to be used for screening range'''
        # For the purposes of the measure, the most recent denominator eligible encounter should
        # be used to determine if the numerator action for the submeasure was performed within
        # the 12-month look back period.
        self.__populace__ = self.__populace__.sort_values(['patient_measurement_year_id','encounter_date']).drop_duplicates(subset=['patient_measurement_year_id'],keep='last').copy()
        self.__populace__['earliest_screening_date'] = self.__populace__['encounter_date'].dt.date - relativedelta(months=12)
        self.__populace__['latest_screening_date'] = self.__populace__['encounter_date'].dt.date        


    @override
    def _find_performance_met(self) -> None:
        '''Finds clients that have been screened within their screening range'''
        screenings = self.__get_systematic_screenings()
        screenings['patient_measurement_year_id'] = self.__create_screening_patient_measurement_year_id(screenings)
        self.__populace__ = self.__populace__.merge(screenings,how='left')
        self.__check_screening_date()

    def __get_systematic_screenings(self) -> pd.DataFrame:
        '''Queries db for all screenings \n
        Returns: screenings'''
        sql = '''
        SELECT
            tblAssessmentToolsAudit.PatientId,
            --tblAssessmentToolsAudit.EncounterID,
            tblEncounterSummary.VisitDateTime AS 'screening_date'
        FROM
            tblAssessmentToolsAudit
        LEFT JOIN
            tblEncounterSummary ON (tblAssessmentToolsAudit.EncounterID = tblEncounterSummary.EncounterId)
        '''
        return pd.read_sql(sql,self.__CONN__)

    def __create_screening_patient_measurement_year_id(self, screenings) -> None:
        '''Creates patient_measurement_year_id'''
        col =  screenings['PatientId'].astype(str) + '-' + screenings['screening_date'].dt.year.astype(str)
        return col.str[:-2]
    
    def __check_screening_date(self) -> None:
        '''Sets Numerator for all clients with a screening during their screening date range'''
        self.__populace__['numerator'] = (self.__populace__['earliest_screening_date'] <= self.__populace__['screening_date']) & (self.__populace__['latest_screening_date'] >= self.__populace__['screening_date'].dt.date)

    @override
    def stratify_data(self) -> None:
        '''Gets stratification for all clients: age, medicaid, ethnicity and race'''
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
        self.__stratify__ = self.__populace__[['patient_measurement_year_id','PatientId','measurement_year','EncounterID','encounter_date']].sort_values(['patient_measurement_year_id','EncounterID']).drop_duplicates('patient_measurement_year_id')

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
                emr_PatientDetails.PatientId IN ({','.join(str(val) for val in self.__stratify__['PatientId'])}) -- creates a list of valid PatientIds
            '''
        patient_data = pd.read_sql(sql,self.__CONN__)
        self.__stratify__ = self.__stratify__.merge(patient_data,how='left')

    def __get_encounter_data(self) -> None:
        '''Doc String'''
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
                PatientId IN ({','.join(str(val) for val in self.__stratify__['PatientId'])}) -- creates a list of valid PatientIds
            '''
        return pd.read_sql(sql,self.__CONN__)

    def __merge_mediciad_with_stratify(self,medicaid_data:pd.DataFrame) -> pd.DataFrame:
        '''Merges stratify data on top of the medicaid data \n
        Returns: merged_data'''
        return medicaid_data.merge(self.__stratify__[['PatientId','encounter_date']])

    def __filter_insurance_dates(self,medicaid_data:pd.DataFrame) -> pd.DataFrame:
        '''Removes insurances that weren't active at the time of the patient's visit \n
        Returns: valid_medicaid'''
        medicaid_data['End'] = medicaid_data['End'].fillna(datetime.now()) # replace nulls with today so that they don't get filtered out
        medicaid_data['valid'] = (medicaid_data['Start'] <= medicaid_data['encounter_date']) & (medicaid_data['End'] >= medicaid_data['encounter_date']) # checks if the insurance is valid at time of encounter
        return medicaid_data[medicaid_data['valid']].copy()

    def __recreate_patient_measurement_year_id(self,medicaid_data:pd.DataFrame) -> pd.Series:
        '''creates the patient measurement year for compatibility with the populace \n
        Returns: patient_measurement_year_id'''
        return (medicaid_data['PatientId'].astype(str) + '-' + medicaid_data['encounter_date'].astype(str)).apply(lambda val: val[:11])

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
        medicaid_data = medicaid_data.merge(self.__stratify__,on=['patient_measurement_year_id'])
        return (medicaid_data.groupby(['patient_measurement_year_id'])['Medicaid'].sum() == 1).reset_index()

    def __fill_blank_stratify(self) -> None:
        '''Fill in all null values with Unknown'''
        self.__stratify__ = self.__stratify__.fillna('Unknown')



    @override
    def return_final_data(self) -> tuple[pd.DataFrame,pd.DataFrame]:
        '''Returns the final calculated data for the ASC Sub 1 measurement \n
        Returns: \n
            tuple[pd.DataFrame, pd.DataFrame]: \n
                - The first DataFrame represents the processed populace data
                - The second DataFrame represents the processed stratified data'''
        self.__trim_unnecessary_populace_data()
        self.__trim_unnecessary_stratify_data()
        return self.__populace__.copy(), self.__stratify__.copy()

    def __trim_unnecessary_populace_data(self) -> None:
        '''Gets rid of all data that isn't needed in the populace'''
        self.__populace__ = self.__populace__[['patient_measurement_year_id','PatientId','EncounterID','screening_date','numerator']].drop_duplicates(subset='patient_measurement_year_id')

    def __trim_unnecessary_stratify_data(self) -> None:
        '''Gets rid of all data that isn't needed to stratify the populace'''
        self.__stratify__ = self.__stratify__[['patient_measurement_year_id','measurement_year','Ethnicity','Race','Medicaid']].drop_duplicates(subset='patient_measurement_year_id')





class _Sub_2 (Submeasure):
    '''Percentage of clients aged 18 years and older who were identified as unhealthy alcohol users
    (in submeasure #1) who received Brief Counseling'''

    def __init__(self,engine:Engine,logger:Logger):
        super().__init__("ASC_sub_2")
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
        '''All clients aged 18 years and older seen for at least two visits or at least one preventive visit
        during the Measurement Year who were screened for unhealthy alcohol use and identified as an
        unhealthy alcohol user'''
        self.__LOGGER__.info("Sub 2 Getting Denominator")
        try:
            super().get_denominator()
            self._get_populace()
            self.__LOGGER__.info("Sub 2 Successfully got Denominator")
        except Exception:
            self.__LOGGER__.error("Sub 2 Failed to get Denominator",exc_info=True)
            raise

    @override
    def _get_populace(self) -> None:
        '''Gets all clients from the submeasure 1 numerator who were identified as unhealthy alchohol users'''
        sub1 = self.__get_sub_1_numerator()
        ids = sub1['PatientId'].to_list()
        uau = self.__get_unhealthy_alchohol_users(ids)
        self.__initialize_populace(sub1,uau)
        self.__populace__['patient_measurement_year_id'] = self.__create_measurement_year_id(self.__populace__['PatientId'],self.__populace__['last_encounter'])

    def __get_sub_1_numerator(self) -> pd.DataFrame:
        '''Queries the db for clients who are part of the submeasure 1 numerator \n
        Returns: sub1_numerator'''
        # patient_measurement_year_id is excluded from the query so that if data come from somewhere else, the code would still be able to break patients up by year
        sql = '''
        SELECT
            --patient_measurement_year_id,
            --screening_date,
            ptASC_sub_1.PatientId,
            tblEncounterSummary.VisitDateTime AS 'last_encounter',
            YEAR(tblEncounterSummary.VisitDateTime) AS 'measurement_year'
        FROM
            ptASC_sub_1
        LEFT JOIN
            tblEncounterSummary ON (ptASC_sub_1.EncounterID = tblEncounterSummary.EncounterId)
        WHERE
            numerator = 1
        '''
        return pd.read_sql(sql,self.__CONN__)

    def __get_unhealthy_alchohol_users(self, sub_1_ids:list) -> pd.DataFrame:
        '''Queries db for all sub1 clients who scored 8 or above on an AUDIT \n
        Parameters: submeasure_1_numerator_ids \n
        Returns: unhealthy_alcohol_users'''
        sql = f'''
        SELECT
            tblAssessmentToolsAudit.PatientId,
            tblAssessmentToolsAudit.EncounterID,
            tblEncounterSummary.VisitDateTime AS 'screening_date',
            tblAssessmentToolsAudit.TotalScore
        FROM
            tblAssessmentToolsAudit
        LEFT JOIN
            tblEncounterSummary ON (tblAssessmentToolsAudit.EncounterID = tblEncounterSummary.EncounterID)
        WHERE
            TotalScore >= 8
            AND
            tblAssessmentToolsAudit.PatientId IN {tuple(sub_1_ids)}
        ORDER BY
            tblAssessmentToolsAudit.PatientId
        '''
        return pd.read_sql(sql,self.__CONN__)

    def __initialize_populace(self, sub1:pd.DataFrame, sub2:pd.DataFrame) -> None:
        '''Creates and filters the denominator populace from submeasure 1's numerator and checks for alchohol screening \n
        Parameters: submeasure_1_numerator
                            unhealthy_alchohol_users'''
        self.__create_populace(sub1,sub2)
        self.__check_screening_date()

    def __create_populace(self, sub1:pd.DataFrame, sub2:pd.DataFrame) -> None:
        '''Puts sub1 data on top of sub2 data to create populace \n
        Parameters: submeasure_1_numerator
                            unhealthy_alchohol_users'''
        self.__populace__ = sub2.merge(sub1)
        
    def __check_screening_date(self) -> None:
        '''Removes clients who's screening was within 1 year of thier last encounter per year'''
        self.__populace__['valid_screening_date'] = (self.__populace__['screening_date'] <= self.__populace__['last_encounter']) & (self.__populace__['last_encounter'].dt.date - relativedelta(years=1) <=self.__populace__['screening_date'])
        self.__populace__ = self.__populace__[self.__populace__['valid_screening_date']].copy()

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
        # Clients with dementia at any time during the patient’s history through the end of the Measurement Year
        # OR
        # Clients who use hospice services any time during the Measurement Year

        # Removing exclusions is not needed as the starting populace pulls data from the sub1 data
        # which already removes exclusions
        pass



    @override
    def get_numerator(self) -> None:
        '''All clients in the denominator who received Brief Counseling'''
        self.__LOGGER__.info("Sub 2 Getting Numerator")
        try:
            super().get_numerator()
            self.__LOGGER__.info("Sub 2 Successfully got Numerator")
        except Exception:
            self.__LOGGER__.error("Sub 2 Failed to get Numerator",exc_info=True)
            raise


    @override
    def _find_performance_met(self) -> None:
        '''Finds all clients the meet the numerator performance'''
        counselings = self.__get_counselings()
        self.__determine_numerator(counselings)

    def __get_counselings(self) -> pd.DataFrame:
        '''Queries db for all clients who've had alchohol counseling \n
        Returns: counselings'''
        sql = '''
        SELECT
            tblTreatmentPlanCustomtabCustomFieldValues.PatientID,
            tblTreatmentPlanCustomtabCustomFieldValues.EncounterID,
            -- tblTreatmentPlanValues.ValueDescription AS 'counseling',
            tblEncounterSummary.VisitDateTime AS 'counsel_date'
            -- ,CAST(tblTreatmentPlanCustomtabCustomFieldValues.PatientID AS varchar) + '-' + CAST(YEAR(tblEncounterSummary.VisitDateTime) AS varchar) AS 'pmy'
        FROM
            tblTreatmentPlanCustomtabCustomFieldValues
        LEFT JOIN
            tblTreatmentPlanValues ON (tblTreatmentPlanCustomtabCustomFieldValues.OrderSetValueID = tblTreatmentPlanValues.OrderSetValueID)
        LEFT JOIN
            tblEncounterSummary ON (tblTreatmentPlanCustomtabCustomFieldValues.EncounterID = tblEncounterSummary.EncounterId)
        WHERE
            tblTreatmentPlanValues.ValueDescription = 'Brief Intervention'
        ORDER BY
            tblTreatmentPlanCustomtabCustomFieldValues.PatientID
        '''
        return pd.read_sql(sql,self.__CONN__).drop_duplicates()

    def __determine_numerator(self,counselings) -> None:
        '''Sets the numerator value for clients that had counseling'''
        self.__populace__['numerator'] = self.__populace__['EncounterID'].isin(counselings['EncounterID'])
        # Questions Related to Timing of Screening and Brief Intervention: (FAQ)
        # Q: Do the screening and brief intervention need to happen in the same session/encounter?
        # A: If you are screening someone for alcohol use, however, the time for a brief intervention is
        # when they are screened. It should happen at the same encounter.

    @override
    def _apply_time_constraint(self) -> None:
        '''Checks to see if the counseling happened within the allotted time span'''
        # NOTE this is not needed, as counseling should happen in the same session as the screening
        #  which is checked in __determine_numerator by encounter_id == encounter_id
        pass



    @override
    def stratify_data(self) -> None:
        '''Gets stratification for all clients: medicaid, ethnicity and race'''
        self.__LOGGER__.info("Sub 2 Getting Stratification")
        try:
            self.__initialize_stratify()
            self.__get_stratify_from_db()
            self.__fill_blank_stratify()
            self.__LOGGER__.info("Sub 2 Successfully got Stratification")
        except Exception:
            self.__LOGGER__.error("Sub 2 Failed to get Stratification",exc_info=True)
            raise

    def __initialize_stratify(self) -> None:
        '''Initializes self.__stratify__ by filtering self.__populace__'''
        self.__stratify__ = self.__populace__[['patient_measurement_year_id','PatientId','measurement_year','EncounterID','screening_date','last_encounter']].sort_values(['patient_measurement_year_id','EncounterID']).drop_duplicates('patient_measurement_year_id')

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
        medicaid_data['patient_measurement_year_id'] = self.__create_measurement_year_id(medicaid_data['PatientId'],medicaid_data['last_encounter'])
        results = self.__determine_medicaid_stratify(medicaid_data)
        self.__stratify__ = self.__stratify__.merge(results)

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
        return medicaid_data.merge(self.__stratify__[['PatientId','screening_date','last_encounter']])

    def __filter_insurance_dates(self,medicaid_data:pd.DataFrame) -> pd.Series:
        '''Removes insurances that weren't active at the time of the patient's visit \n
        Returns: valid_medicaid'''
        medicaid_data['End'] = medicaid_data['End'].fillna(datetime.now()) # replace nulls with today so that they don't get filtered out
        medicaid_data['valid'] = (medicaid_data['Start'] <= medicaid_data['screening_date']) & (medicaid_data['End'] >= medicaid_data['screening_date']) # checks if the insurance is valid at time of encounter
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
        medicaid_data = medicaid_data.merge(self.__stratify__,on=['patient_measurement_year_id'])
        return (medicaid_data.groupby(['patient_measurement_year_id'])['Medicaid'].sum() == 1).reset_index()

    def __fill_blank_stratify(self) -> None:
        '''Fill in all null values with Unknown'''
        self.__stratify__ = self.__stratify__.fillna('Unknown')



    @override
    def return_final_data(self) -> tuple[pd.DataFrame,pd.DataFrame]:
        '''Returns the final calculated data for the ASC Sub 2 measurement \n
        Returns: \n
            tuple[pd.DataFrame, pd.DataFrame]: \n
                - The first DataFrame represents the processed populace data
                - The second DataFrame represents the processed stratified data'''
        self.__trim_unnecessary_populace_data()
        self.__trim_unnecessary_stratify_data()
        return self.__populace__.copy(), self.__stratify__.copy()

    def __trim_unnecessary_populace_data(self) -> None:
        '''Gets rid of all data that isn't needed for the populace'''
        self.__populace__ = self.__populace__[['patient_measurement_year_id','PatientId','EncounterID','numerator']].drop_duplicates(subset='patient_measurement_year_id')

    def __trim_unnecessary_stratify_data(self) -> None:
        '''Gets rid of all data that isn't needed to stratify the denominator'''
        self.__stratify__ = self.__stratify__[['patient_measurement_year_id','measurement_year','Ethnicity','Race','Medicaid']].drop_duplicates(subset='patient_measurement_year_id')




class ASC(Measurement):
    """
    The ASC measure calculates the Percentage of clients aged 18 years and older who were
    screened for unhealthy alcohol use using a Systematic Screening Method at least once within the
    last 12 months AND who received brief counseling if identified as an unhealthy alcohol user
    """

    def __init__(self,engine:Engine,logger:Logger):
        super().__init__("ASC")
        self.__sub1__ = _Sub_1(engine,logger)
        self.__sub2__ = _Sub_2(engine,logger)

    @override
    def get_submeasure_data(self) -> dict[str,pd.DataFrame]:
        """
        Calculates all the data for the ASC Measurement and its Submeasures

        Returns:
            Dictionary[str,pd.DataFrame]
                - str: The name of the submeasure data
                - pd.DataFrame: The data corresponding to that submeasure
        """
        results = {}
        pop,strat = self.__sub1__.collect_measurement_data()
        results[self.__sub1__.get_name()] = pop
        results[self.__sub1__.get_name()+'_stratify'] = strat
        pop,strat = self.__sub2__.collect_measurement_data()
        results[self.__sub2__.get_name()] = pop
        results[self.__sub2__.get_name()+'_stratify'] = strat
        return results