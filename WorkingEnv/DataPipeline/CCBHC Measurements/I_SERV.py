from typing import override
from sqlalchemy import Engine
from datetime import datetime
from logging import Logger
from Measurement import Measurement, Submeasure
import pandas as pd


class _Sub_1(Submeasure):
    '''The I-SERV sub 1 measure calculates the Average time until provision of initial evaluation'''

    def __init__(self,engine:Engine,logger:Logger):
        super().__init__("ISERV_sub_1")
        self.__CONN__ = engine
        self.__LOGGER__ = logger
        self.__EXCLUSIONS__ = None

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
    
    def _get_exclusions_dataframe(self) -> pd.DataFrame:
        '''
        Reurns: calls_without_evals
        '''
        return self.__EXCLUSIONS__.copy()

    @override
    def get_denominator(self) -> None:
        '''The Average number of days until Initial Clinical Service for New Clients'''
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
        self.__initialize_populace()
        self.__calculate_measurement_year()
        self.__create_patient_measurement_year_id()

    def __initialize_populace(self) -> None:
        '''Queries the database for starting populace'''
        calls = self.__get_call_dates()
        evals = self.__get_initial_evals()
        self.__match_evals_to_calls(calls,evals)

    def __get_call_dates(self) -> pd.DataFrame:
        '''Queries the database for all patients' date of call \n
        Returns: calls'''
        sql = '''
            SELECT  tblProgramManagementDetails.PatientID
                    ,ProgramManagementDetailID AS 'CallID'
                    ,StartDate
		            ,emr_PatientDetails.DOB
            FROM
                tblProgramManagementDetails
            LEFT JOIN
                emr_PatientDetails ON (tblProgramManagementDetails.PatientID = emr_PatientDetails.PatientID)
            INNER JOIN
                ptPatient_Activity ON (tblProgramManagementDetails.PatientID = ptPatient_Activity.PatientID)
            WHERE 
                StartDate IS NOT NULL
                AND
                YEAR(StartDate) >= 2024
            '''
        calls = pd.read_sql(sql,self.__CONN__).drop_duplicates()
        calls['StartDate'] = pd.to_datetime(calls['StartDate'])
        return calls
    
    def __get_initial_evals(self) -> pd.DataFrame:
        '''Queries the database for all patients' initial evaluations \n
        Returns: evals'''
        sql = '''
        SELECT 
            PatientId,
            VisitDateTime,
            encounterId AS 'eval_encounter_id'
        FROM
            tblEncounterSummary
        WHERE
            EncounterTypeLong LIKE 'Initial Evaluation-1%'
            AND
            YEAR(VisitDateTime) >= 2024
        '''
        evals = pd.read_sql(sql,self.__CONN__)
        return evals
    
    def __match_evals_to_calls(self, calls, evals) -> None:
        '''Matches initial evalustions to corresponding first calls'''
        # filter out evals of patients who arne't new patients
        call_ids = calls['PatientID'].to_list()
        evals = evals[evals['PatientId'].isin(call_ids)].copy()

        groups = evals.groupby('PatientId')

        # create a mask for patients with evals already vs those without evals
        # skip those without evals, as the lambda will break
        # put them both into populace, needed for "Additional Notes"
        eval_ids = evals['PatientId'].unique()
        eval_mask = calls['PatientID'].isin(eval_ids)
        has_eval = calls[eval_mask].copy()
        has_no_eval = calls[~eval_mask].copy()

        has_eval[['eval_date','eval_encounter_id']] = has_eval.apply(lambda row: self.__get_next_date(row['StartDate'],groups.get_group(row['PatientID'])),axis=1,result_type='expand')
        has_eval = has_eval.drop_duplicates(['PatientID','eval_encounter_id'])

        self.__populace__ = pd.concat([has_eval,has_no_eval])

    def __get_next_date(self, call, evals) -> tuple[pd.Timestamp,int]:
        ''' Finds the next evaluation date and encounter ID after a given call date \n
        Returns: (eval_date,eval_id)'''
        # filters out all evals that occured prior to the call and return the next eval date
        valid_evals = evals[evals['VisitDateTime'] >= call].copy()
        if len(valid_evals['VisitDateTime']) > 0:
            first_row = valid_evals.iloc[0]
            return first_row['VisitDateTime'],first_row['eval_encounter_id']
        else:
            return None, None

    def __calculate_measurement_year(self) -> None:
        '''Calculates the year of the call'''
        self.__populace__['measurement_year'] = pd.to_datetime(self.__populace__['StartDate']).dt.year

    def __create_patient_measurement_year_id(self) -> None:
        '''Creates a unique id to match patients to their coresponding measurement year'''
        self.__populace__['patient_measurement_year_id'] = self.__populace__['PatientID'].astype(str) + '-' + self.__populace__['measurement_year'].astype(str)



    @override
    def _remove_exclusions(self) -> None:
        '''Filters exclusions from populace'''
        self.__prior_visit_exclusions()
        self.__december_exclusions()
        self.__age_exclusions()

    def __prior_visit_exclusions(self) -> None:
        '''Finds and removes patients who have been seen within the past 6 months'''
        # Identify New Clients who contacted the Provider Entity seeking services during the Measurement Year.
        # Note: New Clients are those who have not been served at the clinic in the past six months.
        prior_visits = self.__get_prior_6_month_visits()
        exclusion_ids = self.__create_prior_6_month_id(prior_visits).drop_duplicates().to_list()
        self.__remove_prior_6_month_visits(exclusion_ids)

    def __get_prior_6_month_visits(self) -> pd.DataFrame:
        '''Queries the database for all billable encounters from Jul-Dec \n
        Returns: prior_encounters'''
        sql = '''
        SELECT
            PatientId
            ,YEAR(VisitDateTime) AS 'prior_year'
        FROM
            tblEncounterSummary
        LEFT JOIN
	        tblEncounterType ON (tblEncounterSummary.EncounterTypeId = tblEncounterType.EncounterTypeID)
        WHERE
            MONTH(tblEncounterSummary.VisitDateTime) > 6
	        AND
            YEAR(tblEncounterSummary.VisitDateTime) >= 2023
            AND
            tblEncounterType.IsBillable = 'TRUE'
        '''
        return pd.read_sql(sql,self.__CONN__)

    def __create_prior_6_month_id(self, prior_visits:pd.DataFrame) -> pd.Series:
        '''Creates a unique_patient_id for all prior visits \n
        Returns: exclusion_ids'''
        return prior_visits['PatientId'].astype(str) + '-' + (prior_visits['prior_year'] + 1).astype(str)

    def __remove_prior_6_month_visits(self, exclusion_ids:pd.Series) -> None:
        '''Filters out all measurement year ids of patients who've been seen after July'''
        self.__populace__ = self.__populace__[~self.__populace__['patient_measurement_year_id'].isin(exclusion_ids)].copy()

    def __december_exclusions(self) -> None:
        '''Filters out patients who made first contact during December'''
        # Exclude those who would be New Consumers but who had First Contact during the last 30 days of the MY
        self.__populace__ = self.__populace__[self.__populace__['StartDate'].dt.month != 12].copy()

    def __age_exclusions(self) -> None:
        '''Exclude those age 11 years or younger as of the last day of the MY'''
        self.__calculate_age()
        self.__remove_age()

    def __calculate_age(self) -> None:
        '''Calculates ages based off of the end of the measurement year'''
        # "Identify clients from step 1 of ages 12 years and older as ofthe end of the Measurement Year"
        self.__populace__['age'] = self.__populace__['measurement_year'] - self.__populace__['DOB'].dt.year

    def __remove_age(self) -> None:
        '''Removes all clients under the age of 12'''
        self.__populace__ = self.__populace__[self.__populace__['age'] >= 12].copy()



    @override
    def get_numerator(self) -> None:
        '''The total number of business days between First Contact and Initial Evaluation for all members of the denominator population'''
        self.__LOGGER__.info('Getting Numerator')
        try:
            super().get_numerator()
            self.__LOGGER__.info("Successfully got Numerator")
        except Exception:
            self.__LOGGER__.error('Failed to get Numerator',exc_info=True)
            raise

    @override
    def _apply_time_constraint(self) -> None:
        '''Any patient received an Initial Evaluation after the last day of the Measurement Year are treated as having been evaluated 31 days after First Contact.'''
        self.__populace__['eval_date'] = self.__populace__.apply(lambda row: row['eval_date']
                                                                                if row['eval_date'].year == row['measurement_year']
                                                                                else row['eval_date'] + pd.tseries.offsets.BDay(31),
                                                                                axis=1)
        
    @override
    def _find_performance_met(self) -> None:
        '''Sets the numerator field to the number of business days from call to evaluation'''
        self.__EXCLUSIONS__ = self.__handle_calls_without_evals()
        self.__populace__['business_days_from_call_to_eval'] = self.__calculate_days_from_call()

    def __handle_calls_without_evals(self) -> pd.DataFrame:
        '''Exclude from the Submeasure #1 denominator all eligible New Clients who never received an Initial Evaluation. 
        Indicate in Additional Notes in the data reporting template the number so excluded. \n
        Returns: no_evals'''
        calls = self.__filter_calls_without_evals()
        if len(calls.index) > 0:
            calls = self.__trim_unnecessary_call_data(calls)
            return calls

    def __filter_calls_without_evals(self) -> pd.DataFrame:
        '''Removes patients that never had a initial evaluation \n
        Returns: patients_without_eval '''
        eval_mask = self.__populace__['eval_date'].isnull()
        calls = self.__populace__[eval_mask].copy()
        self.__populace__ = self.__populace__[~eval_mask].copy()
        return calls
    
    def __trim_unnecessary_call_data(self, calls) -> pd.DataFrame:
        '''Gets rid of columns that aren't needed for calls without evals \n
        Returns: calls'''
        return calls[['PatientID','StartDate']]

    def __calculate_days_from_call(self) -> pd.Series:
        '''Calculates the number of business days from the call to the evaluation \n
        Returns: days'''
        # total number of busines days between First Contact and Initial Evaluation

        # pd.bdate_range returns a DateTimeIndex containing all business days within given dates
        # So len(result) gives the number of business days and - 1 removes the date of call
        # i.e. call = mon, eval = tues, number of days = 1
        # pd.bdate_range(call,eval) -> [mon,tues]
        # len(result) -> 2
        # 2 - 1 -> 1
        return self.__populace__.apply(lambda row: len(pd.bdate_range(row['StartDate'],row['eval_date'])) - 1, axis=1)

    

    @override
    def stratify_data(self) -> None:
        '''Gets stratification for all clients: age, medicaid, ethnicity and race'''
        self.__LOGGER__.info('Getting Stratification')
        try:
            self.__initialize_stratify()
            self.__get_age_groups()
            self.__get_stratify_from_db()
            self.__fill_blank_stratify()
            __shared_stratifiy__ = self.__stratify__.copy()
            self.__LOGGER__.info("Successfully got Stratification")
        except Exception:
            self.__LOGGER__.error('Failed to get Stratification',exc_info=True)
            raise

    def __initialize_stratify(self) -> None:
        '''Initializes self.__stratify__ by filtering self.__populace__'''
        self.__stratify__ = self.__populace__[['patient_measurement_year_id','PatientID','StartDate','CallID','eval_encounter_id','age']].copy()

    def __get_age_groups(self) -> None:
        '''Calculates age stratification at the time of index visit'''
        self.__stratify__['age'] = self.__stratify__['age'].apply(lambda age: '18+' if age >= 18 else '12-18')

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
                [emr_PatientDetails].[PatientId] AS 'PatientID',
                [emr_PatientDetails].[EthnicityGroupName] AS 'Ethnicity',
                [emr_PatientRaceGroupTransaction].[RaceGroupName] AS 'Race'
            FROM
                [InSync].[dbo].[emr_PatientDetails]
            LEFT JOIN
                [InSync].[dbo].[emr_PatientRaceGroupTransaction] ON ([InSync].[dbo].[emr_PatientDetails].[PatientId] = [InSync].[dbo].[emr_PatientRaceGroupTransaction].[PatientId])
            WHERE
                [emr_PatientDetails].[PatientId] IN ({','.join(str(val) for val in self.__stratify__['PatientID'])}) -- creates alist of valid PatientIds
            '''
        patient_data = pd.read_sql(sql,self.__CONN__)
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

        eval_ids = self.__stratify__[self.__stratify__['CallID'].notna()]['PatientID']
        sql = f'''
            Select
                [PatientId] AS 'PatientID',
                [EffectiveDate] AS 'Start',
                [DisenrollmentDate] AS 'End',
                LOWER ([tblPayerPlans.PayerPlanName]) AS 'Plan'
            FROM
                [InSync].[dbo].[tblPatientPayers]
            WHERE
                [PatientId] IN ({','.join(str(val) for val in eval_ids)}) -- creates a list of valid PatientIds
            '''
        return pd.read_sql(sql,self.__CONN__)

    def __merge_mediciad_with_stratify(self, medicaid_data:pd.DataFrame) -> pd.DataFrame:
        '''Merges stratify data on top of the medicaid data \n
        Returns: merged_data'''
        return medicaid_data.merge(self.__stratify__[['PatientID','StartDate']])

    def __filter_insurance_dates(self, medicaid_data:pd.DataFrame) -> pd.Series:
        '''Removes insurances that weren't active at the time of the patient's visit \n
        Returns: valid_medicaid'''
        medicaid_data['End'] = medicaid_data['End'].fillna(datetime.now()) # replace nulls with today so that they don't get filtered out
        medicaid_data['valid'] = (medicaid_data['Start'] <= medicaid_data['StartDate']) & (medicaid_data['End'] >= medicaid_data['StartDate']) # checks if the insurance is valid at time of contact
        return medicaid_data[medicaid_data['valid']].copy()

    def __recreate_patient_measurement_year_id(self, medicaid_data:pd.DataFrame) -> pd.Series:
        '''creates the patient measurement year for compatibility with the populace \n
        Returns: patient_measurement_year_id'''
        return (medicaid_data['PatientID'].astype(str) + '-' + medicaid_data['StartDate'].astype(str)).apply(lambda val: val[:11])

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

    def __find_patients_with_only_medicaids(self, medicaid_data:pd.DataFrame) -> pd.DataFrame:
        '''Calcutlates whether a patient has medicaid only or other insurance \n
        Returns: encounter_ids'''
        medicaid_data = medicaid_data.merge(self.__stratify__[['PatientID','StartDate','eval_encounter_id']],on=['PatientID','StartDate'])
        return (medicaid_data.groupby(['eval_encounter_id'])['Medicaid'].sum() == 1).reset_index()

    def __fill_blank_stratify(self) -> None:
        '''Fill in all null values with Unknown'''
        self.__stratify__ = self.__stratify__.fillna('Unknown')



    @override
    def return_final_data(self) -> tuple[pd.DataFrame,pd.DataFrame]:
        '''Returns the final calculated data for the I-SERV Sub 1 measurement \n
        Returns: \n
            tuple[pd.DataFrame, pd.DataFrame]: \n
                - The first DataFrame represents the processed populace data
                - The second DataFrame represents the processed stratified data'''
        self.__trim_unnecessary_populace_data()
        self.__trim_unnecessary_stratify_data()
        return self.__populace__.copy(), self.__stratify__.copy()

    def __trim_unnecessary_populace_data(self) -> None:
        '''Gets rid of all data that isn't needed to calculate the populace'''
        self.__populace__ = self.__populace__[['PatientID', 'patient_measurement_year_id', 'CallID', 'eval_encounter_id','business_days_from_call_to_eval']].drop_duplicates() 

    def __trim_unnecessary_stratify_data(self) -> None:
        '''Gets rid of all data that isn't needed to stratify the denominator numerator'''
        self.__stratify__ = self.__stratify__[['patient_measurement_year_id','age','Ethnicity','Race','Medicaid']].drop_duplicates('patient_measurement_year_id').copy()




class _Sub_2(Submeasure):
    '''The I-SERV sub 2 measure calculates the Average time until provision of initial clinical services'''

    def __init__(self,engine:Engine,logger:Logger):
        super().__init__("ISERV_sub_2")
        self.__CONN__ = engine
        self.__LOGGER__ = logger
        self.__EXCLUSIONS__ = None
    
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
    
    def _get_exclusions_dataframe(self) -> pd.DataFrame:
        '''
        Reurns: calls_without_evals
        '''
        return self.__EXCLUSIONS__.copy()
    
    @override
    def get_denominator(self) -> None:
        '''The Average number of days until Initial Clinical Service for New Clients'''
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
        self.__initialize_populace()
        self.__calculate_measurement_year()

    def __initialize_populace(self) -> None:
        '''Queries the database for starting populace'''
        calls = self.__get_calls()
        service = self.__get_initial_service()
        self.__match_service_to_call(calls,service)

    def __get_calls(self) -> pd.DataFrame:
        '''Queries the database for patients' call data \n
        Returns: calls'''
        sql = '''
        SELECT
            ptISERV_sub1.PatientID,
            ptISERV_sub1.patient_measurement_year_id,
            ptISERV_sub1.CallID,
            tblProgramManagementDetails.StartDate
        FROM
            ptISERV_sub1
        LEFT JOIN
            tblProgramManagementDetails ON (ptISERV_sub1.CallID = tblProgramManagementDetails.ProgramManagementDetailID)
        '''
        return pd.read_sql(sql,self.__CONN__)

    def __get_initial_service(self) -> pd.DataFrame:
        '''Queries the database for patients' service data \n
        Returns: service'''
        sql = '''
        SELECT
            PatientId,
            EncounterId AS 'service_encounter_id',
            VisitDateTime
        FROM
            tblEncounterSummary
        LEFT JOIN
            tblEncounterType ON (tblEncounterSummary.EncounterTypeId = tblEncounterType.EncounterTypeID)
        WHERE
            tblEncounterType.IsBillable = 'TRUE'
            AND
            tblEncounterType.EncounterType NOT LIKE 'Initial Evaluation%'        
            AND
            YEAR(VisitDateTime) >= 2024
        '''
        return pd.read_sql(sql,self.__CONN__)

    def __match_service_to_call(self, calls, service) -> None:
        '''Matches initial evalustions to corresponding first services'''
        # filter out services of patients who arne't new patients
        call_ids = calls['PatientID'].to_list()
        service = service[service['PatientId'].isin(call_ids)].copy()

        groups = service.groupby('PatientId')

        # create a mask for patients with service already vs those without service
        # skip those without service, as the lambda will break
        # put them both into populace, needed for "Additional Notes"
        service_ids = service['PatientId'].unique()
        service_mask = calls['PatientID'].isin(service_ids)
        has_service = calls[service_mask].copy()
        has_no_service = calls[~service_mask].copy()

        has_service[['service_date','service_encounter_id']] = has_service.apply(lambda row: self.__get_next_date(row['StartDate'],groups.get_group(row['PatientID'])),axis=1,result_type='expand')

        self.__populace__ = pd.concat([has_service,has_no_service])

    def __get_next_date(self, call, services) -> tuple[pd.Timestamp,int]:
        ''' Finds the next evaluation date and encounter ID after a given call date \n
        Returns: (eval_date,eval_id)'''
        # filters out all evals that occured prior to the call and return the next eval date
        valid_services = services[services['VisitDateTime'] >= call].copy()
        if len(valid_services['VisitDateTime']) > 0:
            first_row = valid_services.iloc[0]
            return first_row['VisitDateTime'],first_row['service_encounter_id']
        else:
            return None, None

    def __calculate_measurement_year(self):
        '''Calculates the year of the service'''
        self.__populace__['measurement_year'] = self.__populace__['patient_measurement_year_id'].str.slice(-4).astype(int)



    @override
    def _remove_exclusions(self) -> None:
        '''Filters exclusions from populace'''
        # NOTE
        # Removing exclusions is not needed as the starting populace pulls data from the sub1 data
        # which already removes exclusions
        pass



    @override
    def get_numerator(self) -> None:
        '''The total number of days between First Contact and Initial Clinical Service'''
        self.__LOGGER__.info('Getting Numerator')
        try:
            super().get_numerator()
            self.__LOGGER__.info("Successfully got Numerator")
        except Exception:
            self.__LOGGER__.error('Failed to get Denominator',exc_info=True)
            raise

    @override
    def _apply_time_constraint(self) -> None:
        '''Any patient received an Initial Evaluation after the last day of the Measurement Year are treated as having been evaluated 31 days after First Contact.'''
        self.__populace__['service_date'] = self.__populace__.apply(lambda row: row['service_date']
                                                                                if row['service_date'].year == row['measurement_year']
                                                                                else row['service_date'] + pd.tseries.offsets.BDay(31),
                                                                                axis=1)

    @override
    def _find_performance_met(self) -> None:
        '''Sets the numerator field to the number of business days from call to service'''
        self.__EXCLUSIONS__ = self.__handle_calls_without_service()
        self.__populace__['business_days_from_call_to_service'] = self.__calculate_days_from_call()
    
    def __handle_calls_without_service(self) -> pd.DataFrame:
        '''Exclude from the Submeasure #2 denominator all eligible New Clients who never
        received a clinical service. Indicate in Additional Notes in the data reporting template the
        number excluded. \n
        Returns: no_service'''
        calls = self.__filter_calls_without_service()
        if len(calls.index) > 0:
            calls = self.__trim_unnecessary_call_data(calls)
            return calls

    def __filter_calls_without_service(self) -> pd.DataFrame:
        '''Removes patients that never had a initial evaluation \n
        Returns: patients_without_eval '''
        service_mask = self.__populace__['service_date'].isnull()
        calls = self.__populace__[service_mask].copy()
        self.__populace__ = self.__populace__[~service_mask].copy()
        return calls

    def __trim_unnecessary_call_data(self, calls) -> pd.DataFrame:
        '''Gets rid of columns that aren't needed for calls without evals \n
        Returns: calls'''
        return calls[['PatientID','StartDate']]

    def __calculate_days_from_call(self) -> pd.Series:
        '''Calculates the number of business days from the call to the evaluation \n
        Returns: days'''
        # total number of busines days between First Contact and Initial Evaluation

        # pd.bdate_range returns a DateTimeIndex containing all business days within given dates
        # So len(result) gives the number of business days and - 1 removes the date of call
        # i.e. call = mon, eval = tues, number of days = 1
        # pd.bdate_range(call,eval) -> [mon,tues]
        # len(result) -> 2
        # 2 - 1 -> 1
        return self.__populace__.apply(lambda row: len(pd.bdate_range(row['StartDate'],row['service_date'])) - 1, axis=1)



    @override
    def stratify_data(self) -> None:
        '''Gets stratification for all clients: age, medicaid, ethnicity and race'''
        pass
        # self.__LOGGER__.info('Getting Stratification')
        # try:
        #     # NOTE
        #     # sub 2 populace is a subset of sub 1
        #     # so all stratify data should already exist in the db
        #     self.__LOGGER__.info("Successfully got Stratification")
        # except Exception:
        #     self.__LOGGER__.error('Failed to get Denominator',exc_info=True)
        #     raise

    def _set_stratify(self,sub1_stratify:pd.DataFrame) -> None:
        self.__stratify__ = sub1_stratify[sub1_stratify['patient_measurement_year_id'].isin(self.__populace__['patient_measurement_year_id'])].copy()



    @override
    def return_final_data(self) -> tuple[pd.DataFrame,pd.DataFrame]:
        '''Returns the final calculated data for the I-SERV Sub 2 measurement \n
        Returns: \n
            tuple[pd.DataFrame, pd.DataFrame]: \n
                - The first DataFrame represents the processed populace data
                - The second DataFrame represents the processed stratified data'''
        self.__trim_unnecessary_populace_data()
        self.get_stratify_dataframe()
        return self.__populace__.copy(), self.__stratify__.copy()

    def __trim_unnecessary_populace_data(self) -> None:
        '''Gets rid of all data that isn't needed to calculate the populace'''
        self.__populace__ = self.__populace__[['PatientID', 'patient_measurement_year_id', 'StartDate', 'service_encounter_id','business_days_from_call_to_service']].drop_duplicates() 




class I_Serv(Measurement):
    """
    The I-SERV measure calculates the Average time for clients to access three different types of
    services at Behavioral Health Clinics (BHCs) reporting the measure. The I-SERV measure is
    comprised of three sub-measures of time until provision of: (1) initial evaluation, (2) initial
    clinical services, and (3) crisis services
    """

    def __init__(self,engine:Engine,logger:Logger):
        super().__init__("I SERV")
        self.__sub1__ = _Sub_1(engine,logger)
        self.__sub2__ = _Sub_2(engine,logger)
    
    @override
    def get_submeasure_data(self) -> dict[str,pd.DataFrame]:
        """
        Calculates all the data for the I-Serv Measurement and its Submeasures

        Returns:
            Dictionary[str,pd.DataFrame]
                - str: The name of the submeasure data
                - pd.DataFrame: The data corresponding to that submeasure
        """
        results = {}
        pop,strat = self.__sub1__.collect_measurement_data()
        results[self.__sub1__.get_name()] = pop
        results[self.__sub1__.get_name()+'_stratify'] = strat
        results['IServ_sub1_exclusions'] = self.__sub1__._get_exclusions_dataframe() 
        # this should really be resuls[self.__sub1__.get_name()+'_exclusions'] = ...
        # but since my dashboard was built before refactoring I don't want to redo my dashboard (IServ vs ISERV)
        # the code should be changed in the public package
        # ALSO
        # I feel like I should be overriding collect_measurement_data() to return the exclusions as well
        # but that would change the signature and violate LSP... not sure what to do

        self.__sub2__.get_denominator()
        self.__sub2__.get_numerator()
        self.__sub2__._set_stratify(self.__sub1__.get_stratify_dataframe())
        pop,strat = self.__sub2__.return_final_data()
        results[self.__sub2__.get_name()] = pop
        results[self.__sub2__.get_name()+'_stratify'] = strat
        results['IServ_sub2_exclusions'] = self.__sub2__._get_exclusions_dataframe() 
        # see comment above
        return results