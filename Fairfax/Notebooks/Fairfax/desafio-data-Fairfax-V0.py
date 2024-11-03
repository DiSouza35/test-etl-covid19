# Databricks notebook source
# MAGIC %md
# MAGIC ### 0- Introdução
# MAGIC Você foi contratado por uma agência de dados governamentais para projetar e implementar um pipeline de ETL (Extract, Transform, Load) que extrai dados de uma base pública, transforma e carrega os dados para análise em Databricks. Os dados escolhidos são os de COVID-19 disponíveis em https://storage.googleapis.com/covid19-open-data/v3/latest/aggregated.csv.
# MAGIC Após o processo de ETL, você deve realizar uma análise sumária dos dados usando Python e Databricks. A quantidade de sumarização e os objetivos desta sumarização é de sua escolha.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1- Importes, Variáveis e Enums

# COMMAND ----------

# DBTITLE 1,Variáveis

formato = "delta"
outputMode = "overwrite"
url = "https://storage.googleapis.com/covid19-open-data/v3/latest/aggregated.csv" # URL do arquivo CSV
transient_path = '/FileStore/tables/fairfaxDL/transient' # Caminho para salvar o arquivo CSV no Databricks
bronze_path = "/FileStore/tables/fairfaxDL/bronze/"
silver_path = "dbfs:/FileStore/tables/fairfaxDL/silver/"
gold_path = "dbfs:/FileStore/tables/fairfaxDL/gold/"

# COMMAND ----------

# DBTITLE 1,Imports
# MAGIC %pip install unidecode
# MAGIC from pyspark.sql import DataFrame
# MAGIC from enum import Enum
# MAGIC from pyspark.sql.functions import *
# MAGIC import requests
# MAGIC from unidecode import unidecode
# MAGIC import pandas as pd
# MAGIC import matplotlib.pyplot as plt
# MAGIC import pytz
# MAGIC import os

# COMMAND ----------

# DBTITLE 1,Enums
class origemEnum(Enum):
    LOCATION_KEY = 'location_key'
    DATE = 'date'
    MOBILITY_RETAIL_AND_RECREATION = 'mobility_retail_and_recreation'
    MOBILITY_GROCERY_AND_PHARMACY = 'mobility_grocery_and_pharmacy'
    MOBILITY_PARKS = 'mobility_parks'
    MOBILITY_TRANSIT_STATIONS = 'mobility_transit_stations'
    MOBILITY_WORKPLACES = 'mobility_workplaces'
    MOBILITY_RESIDENTIAL = 'mobility_residential'
    SCHOOL_CLOSING = 'school_closing'
    WORKPLACE_CLOSING = 'workplace_closing'
    CANCEL_PUBLIC_EVENTS = 'cancel_public_events'
    RESTRICTIONS_ON_GATHERINGS = 'restrictions_on_gatherings'
    PUBLIC_TRANSPORT_CLOSING = 'public_transport_closing'
    STAY_AT_HOME_REQUIREMENTS = 'stay_at_home_requirements'
    RESTRICTIONS_ON_INTERNAL_MOVEMENT = 'restrictions_on_internal_movement'
    INTERNATIONAL_TRAVEL_CONTROLS = 'international_travel_controls'
    INCOME_SUPPORT = 'income_support'
    DEBT_RELIEF = 'debt_relief'
    FISCAL_MEASURES = 'fiscal_measures'
    INTERNATIONAL_SUPPORT = 'international_support'
    PUBLIC_INFORMATION_CAMPAIGNS = 'public_information_campaigns'
    TESTING_POLICY = 'testing_policy'
    CONTACT_TRACING = 'contact_tracing'
    EMERGENCY_INVESTMENT_IN_HEALTHCARE = 'emergency_investment_in_healthcare'
    INVESTMENT_IN_VACCINES = 'investment_in_vaccines'
    FACIAL_COVERINGS = 'facial_coverings'
    VACCINATION_POLICY = 'vaccination_policy'
    STRINGENCY_INDEX = 'stringency_index'
    LIFE_EXPECTANCY = 'life_expectancy'
    SMOKING_PREVALENCE = 'smoking_prevalence'
    DIABETES_PREVALENCE = 'diabetes_prevalence'
    INFANT_MORTALITY_RATE = 'infant_mortality_rate'
    ADULT_MALE_MORTALITY_RATE = 'adult_male_mortality_rate'
    ADULT_FEMALE_MORTALITY_RATE = 'adult_female_mortality_rate'
    POLLUTION_MORTALITY_RATE = 'pollution_mortality_rate'
    COMORBIDITY_MORTALITY_RATE = 'comorbidity_mortality_rate'
    HOSPITAL_BEDS_PER_1000 = 'hospital_beds_per_1000'
    NURSES_PER_1000 = 'nurses_per_1000'
    PHYSICIANS_PER_1000 = 'physicians_per_1000'
    HEALTH_EXPENDITURE_USD = 'health_expenditure_usd'
    OUT_OF_POCKET_HEALTH_EXPENDITURE_USD = 'out_of_pocket_health_expenditure_usd'
    NEW_CONFIRMED = 'new_confirmed'
    NEW_DECEASED = 'new_deceased'
    NEW_RECOVERED = 'new_recovered'
    NEW_TESTED = 'new_tested'
    CUMULATIVE_CONFIRMED = 'cumulative_confirmed'
    CUMULATIVE_DECEASED = 'cumulative_deceased'
    CUMULATIVE_RECOVERED = 'cumulative_recovered'
    CUMULATIVE_TESTED = 'cumulative_tested'
    AVERAGE_TEMPERATURE_CELSIUS = 'average_temperature_celsius'
    MINIMUM_TEMPERATURE_CELSIUS = 'minimum_temperature_celsius'
    MAXIMUM_TEMPERATURE_CELSIUS = 'maximum_temperature_celsius'
    RAINFALL_MM = 'rainfall_mm'
    SNOWFALL_MM = 'snowfall_mm'
    DEW_POINT = 'dew_point'
    RELATIVE_HUMIDITY = 'relative_humidity'
    OPENSTREETMAP_ID = 'openstreetmap_id'
    LATITUDE = 'latitude'
    LONGITUDE = 'longitude'
    ELEVATION_M = 'elevation_m'
    AREA_SQ_KM = 'area_sq_km'
    AREA_RURAL_SQ_KM = 'area_rural_sq_km'
    AREA_URBAN_SQ_KM = 'area_urban_sq_km'
    NEW_CONFIRMED_AGE_0 = 'new_confirmed_age_0'
    NEW_CONFIRMED_AGE_1 = 'new_confirmed_age_1'
    NEW_CONFIRMED_AGE_2 = 'new_confirmed_age_2'
    NEW_CONFIRMED_AGE_3 = 'new_confirmed_age_3'
    NEW_CONFIRMED_AGE_4 = 'new_confirmed_age_4'
    NEW_CONFIRMED_AGE_5 = 'new_confirmed_age_5'
    NEW_CONFIRMED_AGE_6 = 'new_confirmed_age_6'
    NEW_CONFIRMED_AGE_7 = 'new_confirmed_age_7'
    NEW_CONFIRMED_AGE_8 = 'new_confirmed_age_8'
    NEW_CONFIRMED_AGE_9 = 'new_confirmed_age_9'
    CUMULATIVE_CONFIRMED_AGE_0 = 'cumulative_confirmed_age_0'
    CUMULATIVE_CONFIRMED_AGE_1 = 'cumulative_confirmed_age_1'
    CUMULATIVE_CONFIRMED_AGE_2 = 'cumulative_confirmed_age_2'
    CUMULATIVE_CONFIRMED_AGE_3 = 'cumulative_confirmed_age_3'
    CUMULATIVE_CONFIRMED_AGE_4 = 'cumulative_confirmed_age_4'
    CUMULATIVE_CONFIRMED_AGE_5 = 'cumulative_confirmed_age_5'
    CUMULATIVE_CONFIRMED_AGE_6 = 'cumulative_confirmed_age_6'
    CUMULATIVE_CONFIRMED_AGE_7 = 'cumulative_confirmed_age_7'
    CUMULATIVE_CONFIRMED_AGE_8 = 'cumulative_confirmed_age_8'
    CUMULATIVE_CONFIRMED_AGE_9 = 'cumulative_confirmed_age_9'
    NEW_DECEASED_AGE_0 = 'new_deceased_age_0'
    NEW_DECEASED_AGE_1 = 'new_deceased_age_1'
    NEW_DECEASED_AGE_2 = 'new_deceased_age_2'
    NEW_DECEASED_AGE_3 = 'new_deceased_age_3'
    NEW_DECEASED_AGE_4 = 'new_deceased_age_4'
    NEW_DECEASED_AGE_5 = 'new_deceased_age_5'
    NEW_DECEASED_AGE_6 = 'new_deceased_age_6'
    NEW_DECEASED_AGE_7 = 'new_deceased_age_7'
    NEW_DECEASED_AGE_8 = 'new_deceased_age_8'
    NEW_DECEASED_AGE_9 = 'new_deceased_age_9'
    CUMULATIVE_DECEASED_AGE_0 = 'cumulative_deceased_age_0'
    CUMULATIVE_DECEASED_AGE_1 = 'cumulative_deceased_age_1'
    CUMULATIVE_DECEASED_AGE_2 = 'cumulative_deceased_age_2'
    CUMULATIVE_DECEASED_AGE_3 = 'cumulative_deceased_age_3'
    CUMULATIVE_DECEASED_AGE_4 = 'cumulative_deceased_age_4'
    CUMULATIVE_DECEASED_AGE_5 = 'cumulative_deceased_age_5'
    CUMULATIVE_DECEASED_AGE_6 = 'cumulative_deceased_age_6'
    CUMULATIVE_DECEASED_AGE_7 = 'cumulative_deceased_age_7'
    CUMULATIVE_DECEASED_AGE_8 = 'cumulative_deceased_age_8'
    CUMULATIVE_DECEASED_AGE_9 = 'cumulative_deceased_age_9'
    NEW_RECOVERED_AGE_0 = 'new_recovered_age_0'
    NEW_RECOVERED_AGE_1 = 'new_recovered_age_1'
    NEW_RECOVERED_AGE_2 = 'new_recovered_age_2'
    NEW_RECOVERED_AGE_3 = 'new_recovered_age_3'
    NEW_RECOVERED_AGE_4 = 'new_recovered_age_4'
    NEW_RECOVERED_AGE_5 = 'new_recovered_age_5'
    NEW_RECOVERED_AGE_6 = 'new_recovered_age_6'
    NEW_RECOVERED_AGE_7 = 'new_recovered_age_7'
    NEW_RECOVERED_AGE_8 = 'new_recovered_age_8'
    NEW_RECOVERED_AGE_9 = 'new_recovered_age_9'
    CUMULATIVE_RECOVERED_AGE_0 = 'cumulative_recovered_age_0'
    CUMULATIVE_RECOVERED_AGE_1 = 'cumulative_recovered_age_1'
    CUMULATIVE_RECOVERED_AGE_2 = 'cumulative_recovered_age_2'
    CUMULATIVE_RECOVERED_AGE_3 = 'cumulative_recovered_age_3'
    CUMULATIVE_RECOVERED_AGE_4 = 'cumulative_recovered_age_4'
    CUMULATIVE_RECOVERED_AGE_5 = 'cumulative_recovered_age_5'
    CUMULATIVE_RECOVERED_AGE_6 = 'cumulative_recovered_age_6'
    CUMULATIVE_RECOVERED_AGE_7 = 'cumulative_recovered_age_7'
    CUMULATIVE_RECOVERED_AGE_8 = 'cumulative_recovered_age_8'
    CUMULATIVE_RECOVERED_AGE_9 = 'cumulative_recovered_age_9'
    NEW_TESTED_AGE_0 = 'new_tested_age_0'
    NEW_TESTED_AGE_1 = 'new_tested_age_1'
    NEW_TESTED_AGE_2 = 'new_tested_age_2'
    NEW_TESTED_AGE_3 = 'new_tested_age_3'
    NEW_TESTED_AGE_4 = 'new_tested_age_4'
    NEW_TESTED_AGE_5 = 'new_tested_age_5'
    NEW_TESTED_AGE_6 = 'new_tested_age_6'
    NEW_TESTED_AGE_7 = 'new_tested_age_7'
    NEW_TESTED_AGE_8 = 'new_tested_age_8'
    NEW_TESTED_AGE_9 = 'new_tested_age_9'
    CUMULATIVE_TESTED_AGE_0 = 'cumulative_tested_age_0'
    CUMULATIVE_TESTED_AGE_1 = 'cumulative_tested_age_1'
    CUMULATIVE_TESTED_AGE_2 = 'cumulative_tested_age_2'
    CUMULATIVE_TESTED_AGE_3 = 'cumulative_tested_age_3'
    CUMULATIVE_TESTED_AGE_4 = 'cumulative_tested_age_4'
    CUMULATIVE_TESTED_AGE_5 = 'cumulative_tested_age_5'
    CUMULATIVE_TESTED_AGE_6 = 'cumulative_tested_age_6'
    CUMULATIVE_TESTED_AGE_7 = 'cumulative_tested_age_7'
    CUMULATIVE_TESTED_AGE_8 = 'cumulative_tested_age_8'
    CUMULATIVE_TESTED_AGE_9 = 'cumulative_tested_age_9'
    NEW_HOSPITALIZED_PATIENTS_AGE_0 = 'new_hospitalized_patients_age_0'
    NEW_HOSPITALIZED_PATIENTS_AGE_1 = 'new_hospitalized_patients_age_1'
    NEW_HOSPITALIZED_PATIENTS_AGE_2 = 'new_hospitalized_patients_age_2'
    NEW_HOSPITALIZED_PATIENTS_AGE_3 = 'new_hospitalized_patients_age_3'
    NEW_HOSPITALIZED_PATIENTS_AGE_4 = 'new_hospitalized_patients_age_4'
    NEW_HOSPITALIZED_PATIENTS_AGE_5 = 'new_hospitalized_patients_age_5'
    NEW_HOSPITALIZED_PATIENTS_AGE_6 = 'new_hospitalized_patients_age_6'
    NEW_HOSPITALIZED_PATIENTS_AGE_7 = 'new_hospitalized_patients_age_7'
    NEW_HOSPITALIZED_PATIENTS_AGE_8 = 'new_hospitalized_patients_age_8'
    NEW_HOSPITALIZED_PATIENTS_AGE_9 = 'new_hospitalized_patients_age_9'
    CUMULATIVE_HOSPITALIZED_PATIENTS_AGE_0 = 'cumulative_hospitalized_patients_age_0'
    CUMULATIVE_HOSPITALIZED_PATIENTS_AGE_1 = 'cumulative_hospitalized_patients_age_1'
    CUMULATIVE_HOSPITALIZED_PATIENTS_AGE_2 = 'cumulative_hospitalized_patients_age_2'
    CUMULATIVE_HOSPITALIZED_PATIENTS_AGE_3 = 'cumulative_hospitalized_patients_age_3'
    CUMULATIVE_HOSPITALIZED_PATIENTS_AGE_4 = 'cumulative_hospitalized_patients_age_4'
    CUMULATIVE_HOSPITALIZED_PATIENTS_AGE_5 = 'cumulative_hospitalized_patients_age_5'
    CUMULATIVE_HOSPITALIZED_PATIENTS_AGE_6 = 'cumulative_hospitalized_patients_age_6'
    CUMULATIVE_HOSPITALIZED_PATIENTS_AGE_7 = 'cumulative_hospitalized_patients_age_7'
    CUMULATIVE_HOSPITALIZED_PATIENTS_AGE_8 = 'cumulative_hospitalized_patients_age_8'
    CUMULATIVE_HOSPITALIZED_PATIENTS_AGE_9 = 'cumulative_hospitalized_patients_age_9'
    NEW_INTENSIVE_CARE_PATIENTS_AGE_0 = 'new_intensive_care_patients_age_0'
    NEW_INTENSIVE_CARE_PATIENTS_AGE_1 = 'new_intensive_care_patients_age_1'
    NEW_INTENSIVE_CARE_PATIENTS_AGE_2 = 'new_intensive_care_patients_age_2'
    NEW_INTENSIVE_CARE_PATIENTS_AGE_3 = 'new_intensive_care_patients_age_3'
    NEW_INTENSIVE_CARE_PATIENTS_AGE_4 = 'new_intensive_care_patients_age_4'
    NEW_INTENSIVE_CARE_PATIENTS_AGE_5 = 'new_intensive_care_patients_age_5'
    NEW_INTENSIVE_CARE_PATIENTS_AGE_6 = 'new_intensive_care_patients_age_6'
    NEW_INTENSIVE_CARE_PATIENTS_AGE_7 = 'new_intensive_care_patients_age_7'
    NEW_INTENSIVE_CARE_PATIENTS_AGE_8 = 'new_intensive_care_patients_age_8'
    NEW_INTENSIVE_CARE_PATIENTS_AGE_9 = 'new_intensive_care_patients_age_9'
    CUMULATIVE_INTENSIVE_CARE_PATIENTS_AGE_0 = 'cumulative_intensive_care_patients_age_0'
    CUMULATIVE_INTENSIVE_CARE_PATIENTS_AGE_1 = 'cumulative_intensive_care_patients_age_1'
    CUMULATIVE_INTENSIVE_CARE_PATIENTS_AGE_2 = 'cumulative_intensive_care_patients_age_2'
    CUMULATIVE_INTENSIVE_CARE_PATIENTS_AGE_3 = 'cumulative_intensive_care_patients_age_3'
    CUMULATIVE_INTENSIVE_CARE_PATIENTS_AGE_4 = 'cumulative_intensive_care_patients_age_4'
    CUMULATIVE_INTENSIVE_CARE_PATIENTS_AGE_5 = 'cumulative_intensive_care_patients_age_5'
    CUMULATIVE_INTENSIVE_CARE_PATIENTS_AGE_6 = 'cumulative_intensive_care_patients_age_6'
    CUMULATIVE_INTENSIVE_CARE_PATIENTS_AGE_7 = 'cumulative_intensive_care_patients_age_7'
    CUMULATIVE_INTENSIVE_CARE_PATIENTS_AGE_8 = 'cumulative_intensive_care_patients_age_8'
    CUMULATIVE_INTENSIVE_CARE_PATIENTS_AGE_9 = 'cumulative_intensive_care_patients_age_9'
    NEW_VENTILATOR_PATIENTS_AGE_0 = 'new_ventilator_patients_age_0'
    NEW_VENTILATOR_PATIENTS_AGE_1 = 'new_ventilator_patients_age_1'
    NEW_VENTILATOR_PATIENTS_AGE_2 = 'new_ventilator_patients_age_2'
    NEW_VENTILATOR_PATIENTS_AGE_3 = 'new_ventilator_patients_age_3'
    NEW_VENTILATOR_PATIENTS_AGE_4 = 'new_ventilator_patients_age_4'
    NEW_VENTILATOR_PATIENTS_AGE_5 = 'new_ventilator_patients_age_5'
    NEW_VENTILATOR_PATIENTS_AGE_6 = 'new_ventilator_patients_age_6'
    NEW_VENTILATOR_PATIENTS_AGE_7 = 'new_ventilator_patients_age_7'
    NEW_VENTILATOR_PATIENTS_AGE_8 = 'new_ventilator_patients_age_8'
    NEW_VENTILATOR_PATIENTS_AGE_9 = 'new_ventilator_patients_age_9'
    CUMULATIVE_VENTILATOR_PATIENTS_AGE_0 = 'cumulative_ventilator_patients_age_0'
    CUMULATIVE_VENTILATOR_PATIENTS_AGE_1 = 'cumulative_ventilator_patients_age_1'
    CUMULATIVE_VENTILATOR_PATIENTS_AGE_2 = 'cumulative_ventilator_patients_age_2'
    CUMULATIVE_VENTILATOR_PATIENTS_AGE_3 = 'cumulative_ventilator_patients_age_3'
    CUMULATIVE_VENTILATOR_PATIENTS_AGE_4 = 'cumulative_ventilator_patients_age_4'
    CUMULATIVE_VENTILATOR_PATIENTS_AGE_5 = 'cumulative_ventilator_patients_age_5'
    CUMULATIVE_VENTILATOR_PATIENTS_AGE_6 = 'cumulative_ventilator_patients_age_6'
    CUMULATIVE_VENTILATOR_PATIENTS_AGE_7 = 'cumulative_ventilator_patients_age_7'
    CUMULATIVE_VENTILATOR_PATIENTS_AGE_8 = 'cumulative_ventilator_patients_age_8'
    CUMULATIVE_VENTILATOR_PATIENTS_AGE_9 = 'cumulative_ventilator_patients_age_9'
    AGE_BIN_0 = 'age_bin_0'
    AGE_BIN_1 = 'age_bin_1'
    AGE_BIN_2 = 'age_bin_2'
    AGE_BIN_3 = 'age_bin_3'
    AGE_BIN_4 = 'age_bin_4'
    AGE_BIN_5 = 'age_bin_5'
    AGE_BIN_6 = 'age_bin_6'
    AGE_BIN_7 = 'age_bin_7'
    AGE_BIN_8 = 'age_bin_8'
    AGE_BIN_9 = 'age_bin_9'
    NEW_HOSPITALIZED_PATIENTS = 'new_hospitalized_patients'
    CUMULATIVE_HOSPITALIZED_PATIENTS = 'cumulative_hospitalized_patients'
    CURRENT_HOSPITALIZED_PATIENTS = 'current_hospitalized_patients'
    NEW_INTENSIVE_CARE_PATIENTS = 'new_intensive_care_patients'
    CUMULATIVE_INTENSIVE_CARE_PATIENTS = 'cumulative_intensive_care_patients'
    CURRENT_INTENSIVE_CARE_PATIENTS = 'current_intensive_care_patients'
    NEW_VENTILATOR_PATIENTS = 'new_ventilator_patients'
    CUMULATIVE_VENTILATOR_PATIENTS = 'cumulative_ventilator_patients'
    CURRENT_VENTILATOR_PATIENTS = 'current_ventilator_patients'
    NEW_PERSONS_VACCINATED = 'new_persons_vaccinated'
    CUMULATIVE_PERSONS_VACCINATED = 'cumulative_persons_vaccinated'
    NEW_PERSONS_FULLY_VACCINATED = 'new_persons_fully_vaccinated'
    CUMULATIVE_PERSONS_FULLY_VACCINATED = 'cumulative_persons_fully_vaccinated'
    NEW_VACCINE_DOSES_ADMINISTERED = 'new_vaccine_doses_administered'
    CUMULATIVE_VACCINE_DOSES_ADMINISTERED = 'cumulative_vaccine_doses_administered'
    NEW_PERSONS_VACCINATED_PFIZER = 'new_persons_vaccinated_pfizer'
    CUMULATIVE_PERSONS_VACCINATED_PFIZER = 'cumulative_persons_vaccinated_pfizer'
    NEW_PERSONS_FULLY_VACCINATED_PFIZER = 'new_persons_fully_vaccinated_pfizer'
    CUMULATIVE_PERSONS_FULLY_VACCINATED_PFIZER = 'cumulative_persons_fully_vaccinated_pfizer'
    NEW_VACCINE_DOSES_ADMINISTERED_PFIZER = 'new_vaccine_doses_administered_pfizer'
    CUMULATIVE_VACCINE_DOSES_ADMINISTERED_PFIZER = 'cumulative_vaccine_doses_administered_pfizer'
    NEW_PERSONS_VACCINATED_MODERNA = 'new_persons_vaccinated_moderna'
    CUMULATIVE_PERSONS_VACCINATED_MODERNA = 'cumulative_persons_vaccinated_moderna'
    NEW_PERSONS_FULLY_VACCINATED_MODERNA = 'new_persons_fully_vaccinated_moderna'
    CUMULATIVE_PERSONS_FULLY_VACCINATED_MODERNA = 'cumulative_persons_fully_vaccinated_moderna'
    NEW_VACCINE_DOSES_ADMINISTERED_MODERNA = 'new_vaccine_doses_administered_moderna'
    CUMULATIVE_VACCINE_DOSES_ADMINISTERED_MODERNA = 'cumulative_vaccine_doses_administered_moderna'
    NEW_PERSONS_VACCINATED_JANSSEN = 'new_persons_vaccinated_janssen'
    CUMULATIVE_PERSONS_VACCINATED_JANSSEN = 'cumulative_persons_vaccinated_janssen'
    NEW_PERSONS_FULLY_VACCINATED_JANSSEN = 'new_persons_fully_vaccinated_janssen'
    CUMULATIVE_PERSONS_FULLY_VACCINATED_JANSSEN = 'cumulative_persons_fully_vaccinated_janssen'
    NEW_VACCINE_DOSES_ADMINISTERED_JANSSEN = 'new_vaccine_doses_administered_janssen'
    CUMULATIVE_VACCINE_DOSES_ADMINISTERED_JANSSEN = 'cumulative_vaccine_doses_administered_janssen'
    NEW_PERSONS_VACCINATED_SINOVAC = 'new_persons_vaccinated_sinovac'
    TOTAL_PERSONS_VACCINATED_SINOVAC = 'total_persons_vaccinated_sinovac'
    NEW_PERSONS_FULLY_VACCINATED_SINOVAC = 'new_persons_fully_vaccinated_sinovac'
    TOTAL_PERSONS_FULLY_VACCINATED_SINOVAC = 'total_persons_fully_vaccinated_sinovac'
    NEW_VACCINE_DOSES_ADMINISTERED_SINOVAC = 'new_vaccine_doses_administered_sinovac'
    TOTAL_VACCINE_DOSES_ADMINISTERED_SINOVAC = 'total_vaccine_doses_administered_sinovac'
    PLACE_ID = 'place_id'
    WIKIDATA_ID = 'wikidata_id'
    DATACOMMONS_ID = 'datacommons_id'
    COUNTRY_CODE = 'country_code'
    COUNTRY_NAME = 'country_name'
    SUBREGION1_CODE = 'subregion1_code'
    SUBREGION1_NAME = 'subregion1_name'
    SUBREGION2_CODE = 'subregion2_code'
    SUBREGION2_NAME = 'subregion2_name'
    LOCALITY_CODE = 'locality_code'
    LOCALITY_NAME = 'locality_name'
    ISO_3166_1_ALPHA_2 = 'iso_3166_1_alpha_2'
    ISO_3166_1_ALPHA_3 = 'iso_3166_1_alpha_3'
    AGGREGATION_LEVEL = 'aggregation_level'
    GDP_USD = 'gdp_usd'
    GDP_PER_CAPITA_USD = 'gdp_per_capita_usd'
    HUMAN_CAPITAL_INDEX = 'human_capital_index'
    POPULATION = 'population'
    POPULATION_MALE = 'population_male'
    POPULATION_FEMALE = 'population_female'
    POPULATION_RURAL = 'population_rural'
    POPULATION_URBAN = 'population_urban'
    POPULATION_LARGEST_CITY = 'population_largest_city'
    POPULATION_CLUSTERED = 'population_clustered'
    POPULATION_DENSITY = 'population_density'
    HUMAN_DEVELOPMENT_INDEX = 'human_development_index'
    POPULATION_AGE_00_09 = 'population_age_00_09'
    POPULATION_AGE_10_19 = 'population_age_10_19'
    POPULATION_AGE_20_29 = 'population_age_20_29'
    POPULATION_AGE_30_39 = 'population_age_30_39'
    POPULATION_AGE_40_49 = 'population_age_40_49'
    POPULATION_AGE_50_59 = 'population_age_50_59'
    POPULATION_AGE_60_69 = 'population_age_60_69'
    POPULATION_AGE_70_79 = 'population_age_70_79'
    POPULATION_AGE_80_AND_OLDER = 'population_age_80_and_older'
    NEW_CONFIRMED_MALE = 'new_confirmed_male'
    NEW_CONFIRMED_FEMALE = 'new_confirmed_female'
    CUMULATIVE_CONFIRMED_MALE = 'cumulative_confirmed_male'
    CUMULATIVE_CONFIRMED_FEMALE = 'cumulative_confirmed_female'
    NEW_DECEASED_MALE = 'new_deceased_male'
    NEW_DECEASED_FEMALE = 'new_deceased_female'
    CUMULATIVE_DECEASED_MALE = 'cumulative_deceased_male'
    CUMULATIVE_DECEASED_FEMALE = 'cumulative_deceased_female'
    NEW_RECOVERED_MALE = 'new_recovered_male'
    NEW_RECOVERED_FEMALE = 'new_recovered_female'
    CUMULATIVE_RECOVERED_MALE = 'cumulative_recovered_male'
    CUMULATIVE_RECOVERED_FEMALE = 'cumulative_recovered_female'
    NEW_TESTED_MALE = 'new_tested_male'
    NEW_TESTED_FEMALE = 'new_tested_female'
    CUMULATIVE_TESTED_MALE = 'cumulative_tested_male'
    CUMULATIVE_TESTED_FEMALE = 'cumulative_tested_female'
    NEW_HOSPITALIZED_PATIENTS_MALE = 'new_hospitalized_patients_male'
    NEW_HOSPITALIZED_PATIENTS_FEMALE = 'new_hospitalized_patients_female'
    CUMULATIVE_HOSPITALIZED_PATIENTS_MALE = 'cumulative_hospitalized_patients_male'
    CUMULATIVE_HOSPITALIZED_PATIENTS_FEMALE = 'cumulative_hospitalized_patients_female'
    NEW_INTENSIVE_CARE_PATIENTS_MALE = 'new_intensive_care_patients_male'
    NEW_INTENSIVE_CARE_PATIENTS_FEMALE = 'new_intensive_care_patients_female'
    CUMULATIVE_INTENSIVE_CARE_PATIENTS_MALE = 'cumulative_intensive_care_patients_male'
    CUMULATIVE_INTENSIVE_CARE_PATIENTS_FEMALE = 'cumulative_intensive_care_patients_female'
    NEW_VENTILATOR_PATIENTS_MALE = 'new_ventilator_patients_male'
    NEW_VENTILATOR_PATIENTS_FEMALE = 'new_ventilator_patients_female'
    CUMULATIVE_VENTILATOR_PATIENTS_MALE = 'cumulative_ventilator_patients_male'
    CUMULATIVE_VENTILATOR_PATIENTS_FEMALE = 'cumulative_ventilator_patients_female'
    SEARCH_TRENDS_ABDOMINAL_OBESITY = 'search_trends_abdominal_obesity'
    SEARCH_TRENDS_ABDOMINAL_PAIN = 'search_trends_abdominal_pain'
    SEARCH_TRENDS_ACNE = 'search_trends_acne'
    SEARCH_TRENDS_ACTINIC_KERATOSIS = 'search_trends_actinic_keratosis'
    SEARCH_TRENDS_ACUTE_BRONCHITIS = 'search_trends_acute_bronchitis'
    SEARCH_TRENDS_ADRENAL_CRISIS = 'search_trends_adrenal_crisis'
    SEARCH_TRENDS_AGEUSIA = 'search_trends_ageusia'
    SEARCH_TRENDS_ALCOHOLISM = 'search_trends_alcoholism'
    SEARCH_TRENDS_ALLERGIC_CONJUNCTIVITIS = 'search_trends_allergic_conjunctivitis'
    SEARCH_TRENDS_ALLERGY = 'search_trends_allergy'
    SEARCH_TRENDS_AMBLYOPIA = 'search_trends_amblyopia'
    SEARCH_TRENDS_AMENORRHEA = 'search_trends_amenorrhea'
    SEARCH_TRENDS_AMNESIA = 'search_trends_amnesia'
    SEARCH_TRENDS_ANAL_FISSURE = 'search_trends_anal_fissure'
    SEARCH_TRENDS_ANAPHYLAXIS = 'search_trends_anaphylaxis'
    SEARCH_TRENDS_ANEMIA = 'search_trends_anemia'
    SEARCH_TRENDS_ANGINA_PECTORIS = 'search_trends_angina_pectoris'
    SEARCH_TRENDS_ANGIOEDEMA = 'search_trends_angioedema'
    SEARCH_TRENDS_ANGULAR_CHEILITIS = 'search_trends_angular_cheilitis'
    SEARCH_TRENDS_ANOSMIA = 'search_trends_anosmia'
    SEARCH_TRENDS_ANXIETY = 'search_trends_anxiety'
    SEARCH_TRENDS_APHASIA = 'search_trends_aphasia'
    SEARCH_TRENDS_APHONIA = 'search_trends_aphonia'
    SEARCH_TRENDS_APNEA = 'search_trends_apnea'
    SEARCH_TRENDS_ARTHRALGIA = 'search_trends_arthralgia'
    SEARCH_TRENDS_ARTHRITIS = 'search_trends_arthritis'
    SEARCH_TRENDS_ASCITES = 'search_trends_ascites'
    SEARCH_TRENDS_ASPERGER_SYNDROME = 'search_trends_asperger_syndrome'
    SEARCH_TRENDS_ASPHYXIA = 'search_trends_asphyxia'
    SEARCH_TRENDS_ASTHMA = 'search_trends_asthma'
    SEARCH_TRENDS_ASTIGMATISM = 'search_trends_astigmatism'
    SEARCH_TRENDS_ATAXIA = 'search_trends_ataxia'
    SEARCH_TRENDS_ATHEROMA = 'search_trends_atheroma'
    SEARCH_TRENDS_ATTENTION_DEFICIT_HYPERACTIVITY_DISORDER = 'search_trends_attention_deficit_hyperactivity_disorder'
    SEARCH_TRENDS_AUDITORY_HALLUCINATION = 'search_trends_auditory_hallucination'
    SEARCH_TRENDS_AUTOIMMUNE_DISEASE = 'search_trends_autoimmune_disease'
    SEARCH_TRENDS_AVOIDANT_PERSONALITY_DISORDER = 'search_trends_avoidant_personality_disorder'
    SEARCH_TRENDS_BACK_PAIN = 'search_trends_back_pain'
    SEARCH_TRENDS_BACTERIAL_VAGINOSIS = 'search_trends_bacterial_vaginosis'
    SEARCH_TRENDS_BALANCE_DISORDER = 'search_trends_balance_disorder'
    SEARCH_TRENDS_BEAUS_LINES = 'search_trends_beaus_lines'
    SEARCH_TRENDS_BELLS_PALSY = 'search_trends_bells_palsy'
    SEARCH_TRENDS_BILIARY_COLIC = 'search_trends_biliary_colic'
    SEARCH_TRENDS_BINGE_EATING = 'search_trends_binge_eating'
    SEARCH_TRENDS_BLEEDING = 'search_trends_bleeding'
    SEARCH_TRENDS_BLEEDING_ON_PROBING = 'search_trends_bleeding_on_probing'
    SEARCH_TRENDS_BLEPHAROSPASM = 'search_trends_blepharospasm'
    SEARCH_TRENDS_BLOATING = 'search_trends_bloating'
    SEARCH_TRENDS_BLOOD_IN_STOOL = 'search_trends_blood_in_stool'
    SEARCH_TRENDS_BLURRED_VISION = 'search_trends_blurred_vision'
    SEARCH_TRENDS_BLUSHING = 'search_trends_blushing'
    SEARCH_TRENDS_BOIL = 'search_trends_boil'
    SEARCH_TRENDS_BONE_FRACTURE = 'search_trends_bone_fracture'
    SEARCH_TRENDS_BONE_TUMOR = 'search_trends_bone_tumor'
    SEARCH_TRENDS_BOWEL_OBSTRUCTION = 'search_trends_bowel_obstruction'
    SEARCH_TRENDS_BRADYCARDIA = 'search_trends_bradycardia'
    SEARCH_TRENDS_BRAXTON_HICKS_CONTRACTIONS = 'search_trends_braxton_hicks_contractions'
    SEARCH_TRENDS_BREAKTHROUGH_BLEEDING = 'search_trends_breakthrough_bleeding'
    SEARCH_TRENDS_BREAST_PAIN = 'search_trends_breast_pain'
    SEARCH_TRENDS_BRONCHITIS = 'search_trends_bronchitis'
    SEARCH_TRENDS_BRUISE = 'search_trends_bruise'
    SEARCH_TRENDS_BRUXISM = 'search_trends_bruxism'
    SEARCH_TRENDS_BUNION = 'search_trends_bunion'
    SEARCH_TRENDS_BURN = 'search_trends_burn'
    SEARCH_TRENDS_BURNING_CHEST_PAIN = 'search_trends_burning_chest_pain'
    SEARCH_TRENDS_BURNING_MOUTH_SYNDROME = 'search_trends_burning_mouth_syndrome'
    SEARCH_TRENDS_CANDIDIASIS = 'search_trends_candidiasis'
    SEARCH_TRENDS_CANKER_SORE = 'search_trends_canker_sore'
    SEARCH_TRENDS_CARDIAC_ARREST = 'search_trends_cardiac_arrest'
    SEARCH_TRENDS_CARPAL_TUNNEL_SYNDROME = 'search_trends_carpal_tunnel_syndrome'
    SEARCH_TRENDS_CATAPLEXY = 'search_trends_cataplexy'
    SEARCH_TRENDS_CATARACT = 'search_trends_cataract'
    SEARCH_TRENDS_CHANCRE = 'search_trends_chancre'
    SEARCH_TRENDS_CHEILITIS = 'search_trends_cheilitis'
    SEARCH_TRENDS_CHEST_PAIN = 'search_trends_chest_pain'
    SEARCH_TRENDS_CHILLS = 'search_trends_chills'
    SEARCH_TRENDS_CHOREA = 'search_trends_chorea'
    SEARCH_TRENDS_CHRONIC_PAIN = 'search_trends_chronic_pain'
    SEARCH_TRENDS_CIRRHOSIS = 'search_trends_cirrhosis'
    SEARCH_TRENDS_CLEFT_LIP_AND_CLEFT_PALATE = 'search_trends_cleft_lip_and_cleft_palate'
    SEARCH_TRENDS_CLOUDING_OF_CONSCIOUSNESS = 'search_trends_clouding_of_consciousness'
    SEARCH_TRENDS_CLUSTER_HEADACHE = 'search_trends_cluster_headache'
    SEARCH_TRENDS_COLITIS = 'search_trends_colitis'
    SEARCH_TRENDS_COMA = 'search_trends_coma'
    SEARCH_TRENDS_COMMON_COLD = 'search_trends_common_cold'
    SEARCH_TRENDS_COMPULSIVE_BEHAVIOR = 'search_trends_compulsive_behavior'
    SEARCH_TRENDS_COMPULSIVE_HOARDING = 'search_trends_compulsive_hoarding'
    SEARCH_TRENDS_CONFUSION = 'search_trends_confusion'
    SEARCH_TRENDS_CONGENITAL_HEART_DEFECT = 'search_trends_congenital_heart_defect'
    SEARCH_TRENDS_CONJUNCTIVITIS = 'search_trends_conjunctivitis'
    SEARCH_TRENDS_CONSTIPATION = 'search_trends_constipation'
    SEARCH_TRENDS_CONVULSION = 'search_trends_convulsion'
    SEARCH_TRENDS_COUGH = 'search_trends_cough'
    SEARCH_TRENDS_CRACKLES = 'search_trends_crackles'
    SEARCH_TRENDS_CRAMP = 'search_trends_cramp'
    SEARCH_TRENDS_CREPITUS = 'search_trends_crepitus'
    SEARCH_TRENDS_CROUP = 'search_trends_croup'
    SEARCH_TRENDS_CYANOSIS = 'search_trends_cyanosis'
    SEARCH_TRENDS_DANDRUFF = 'search_trends_dandruff'
    SEARCH_TRENDS_DELAYED_ONSET_MUSCLE_SORENESS = 'search_trends_delayed_onset_muscle_soreness'
    SEARCH_TRENDS_DEMENTIA = 'search_trends_dementia'
    SEARCH_TRENDS_DENTIN_HYPERSENSITIVITY = 'search_trends_dentin_hypersensitivity'
    SEARCH_TRENDS_DEPERSONALIZATION = 'search_trends_depersonalization'
    SEARCH_TRENDS_DEPRESSION = 'search_trends_depression'
    SEARCH_TRENDS_DERMATITIS = 'search_trends_dermatitis'
    SEARCH_TRENDS_DESQUAMATION = 'search_trends_desquamation'
    SEARCH_TRENDS_DEVELOPMENTAL_DISABILITY = 'search_trends_developmental_disability'
    SEARCH_TRENDS_DIABETES = 'search_trends_diabetes'
    SEARCH_TRENDS_DIABETIC_KETOACIDOSIS = 'search_trends_diabetic_ketoacidosis'
    SEARCH_TRENDS_DIARRHEA = 'search_trends_diarrhea'
    SEARCH_TRENDS_DIZZINESS = 'search_trends_dizziness'
    SEARCH_TRENDS_DRY_EYE_SYNDROME = 'search_trends_dry_eye_syndrome'
    SEARCH_TRENDS_DYSAUTONOMIA = 'search_trends_dysautonomia'
    SEARCH_TRENDS_DYSGEUSIA = 'search_trends_dysgeusia'
    SEARCH_TRENDS_DYSMENORRHEA = 'search_trends_dysmenorrhea'
    SEARCH_TRENDS_DYSPAREUNIA = 'search_trends_dyspareunia'
    SEARCH_TRENDS_DYSPHAGIA = 'search_trends_dysphagia'
    SEARCH_TRENDS_DYSPHORIA = 'search_trends_dysphoria'
    SEARCH_TRENDS_DYSTONIA = 'search_trends_dystonia'
    SEARCH_TRENDS_DYSURIA = 'search_trends_dysuria'
    SEARCH_TRENDS_EAR_PAIN = 'search_trends_ear_pain'
    SEARCH_TRENDS_ECZEMA = 'search_trends_eczema'
    SEARCH_TRENDS_EDEMA = 'search_trends_edema'
    SEARCH_TRENDS_ENCEPHALITIS = 'search_trends_encephalitis'
    SEARCH_TRENDS_ENCEPHALOPATHY = 'search_trends_encephalopathy'
    SEARCH_TRENDS_EPIDERMOID_CYST = 'search_trends_epidermoid_cyst'
    SEARCH_TRENDS_EPILEPSY = 'search_trends_epilepsy'
    SEARCH_TRENDS_EPIPHORA = 'search_trends_epiphora'
    SEARCH_TRENDS_ERECTILE_DYSFUNCTION = 'search_trends_erectile_dysfunction'
    SEARCH_TRENDS_ERYTHEMA = 'search_trends_erythema'
    SEARCH_TRENDS_ERYTHEMA_CHRONICUM_MIGRANS = 'search_trends_erythema_chronicum_migrans'
    SEARCH_TRENDS_ESOPHAGITIS = 'search_trends_esophagitis'
    SEARCH_TRENDS_EXCESSIVE_DAYTIME_SLEEPINESS = 'search_trends_excessive_daytime_sleepiness'
    SEARCH_TRENDS_EYE_PAIN = 'search_trends_eye_pain'
    SEARCH_TRENDS_EYE_STRAIN = 'search_trends_eye_strain'
    SEARCH_TRENDS_FACIAL_NERVE_PARALYSIS = 'search_trends_facial_nerve_paralysis'
    SEARCH_TRENDS_FACIAL_SWELLING = 'search_trends_facial_swelling'
    SEARCH_TRENDS_FASCICULATION = 'search_trends_fasciculation'
    SEARCH_TRENDS_FATIGUE = 'search_trends_fatigue'
    SEARCH_TRENDS_FATTY_LIVER_DISEASE = 'search_trends_fatty_liver_disease'
    SEARCH_TRENDS_FECAL_INCONTINENCE = 'search_trends_fecal_incontinence'
    SEARCH_TRENDS_FEVER = 'search_trends_fever'
    SEARCH_TRENDS_FIBRILLATION = 'search_trends_fibrillation'
    SEARCH_TRENDS_FIBROCYSTIC_BREAST_CHANGES = 'search_trends_fibrocystic_breast_changes'
    SEARCH_TRENDS_FIBROMYALGIA = 'search_trends_fibromyalgia'
    SEARCH_TRENDS_FLATULENCE = 'search_trends_flatulence'
    SEARCH_TRENDS_FLOATER = 'search_trends_floater'
    SEARCH_TRENDS_FOCAL_SEIZURE = 'search_trends_focal_seizure'
    SEARCH_TRENDS_FOLATE_DEFICIENCY = 'search_trends_folate_deficiency'
    SEARCH_TRENDS_FOOD_CRAVING = 'search_trends_food_craving'
    SEARCH_TRENDS_FOOD_INTOLERANCE = 'search_trends_food_intolerance'
    SEARCH_TRENDS_FREQUENT_URINATION = 'search_trends_frequent_urination'
    SEARCH_TRENDS_GASTROESOPHAGEAL_REFLUX_DISEASE = 'search_trends_gastroesophageal_reflux_disease'
    SEARCH_TRENDS_GASTROPARESIS = 'search_trends_gastroparesis'
    SEARCH_TRENDS_GENERALIZED_ANXIETY_DISORDER = 'search_trends_generalized_anxiety_disorder'
    SEARCH_TRENDS_GENERALIZED_TONIC_CLONIC_SEIZURE = 'search_trends_generalized_tonic–clonic_seizure'
    SEARCH_TRENDS_GENITAL_WART = 'search_trends_genital_wart'
    SEARCH_TRENDS_GINGIVAL_RECESSION = 'search_trends_gingival_recession'
    SEARCH_TRENDS_GINGIVITIS = 'search_trends_gingivitis'
    SEARCH_TRENDS_GLOBUS_PHARYNGIS = 'search_trends_globus_pharyngis'
    SEARCH_TRENDS_GOITRE = 'search_trends_goitre'
    SEARCH_TRENDS_GOUT = 'search_trends_gout'
    SEARCH_TRENDS_GRANDIOSITY = 'search_trends_grandiosity'
    SEARCH_TRENDS_GRANULOMA = 'search_trends_granuloma'
    SEARCH_TRENDS_GUILT = 'search_trends_guilt'
    SEARCH_TRENDS_HAIR_LOSS = 'search_trends_hair_loss'
    SEARCH_TRENDS_HALITOSIS = 'search_trends_halitosis'
    SEARCH_TRENDS_HAY_FEVER = 'search_trends_hay_fever'
    SEARCH_TRENDS_HEADACHE = 'search_trends_headache'
    SEARCH_TRENDS_HEART_ARRHYTHMIA = 'search_trends_heart_arrhythmia'
    SEARCH_TRENDS_HEART_MURMUR = 'search_trends_heart_murmur'
    SEARCH_TRENDS_HEARTBURN = 'search_trends_heartburn'
    SEARCH_TRENDS_HEMATOCHEZIA = 'search_trends_hematochezia'
    SEARCH_TRENDS_HEMATOMA = 'search_trends_hematoma'
    SEARCH_TRENDS_HEMATURIA = 'search_trends_hematuria'
    SEARCH_TRENDS_HEMOLYSIS = 'search_trends_hemolysis'
    SEARCH_TRENDS_HEMOPTYSIS = 'search_trends_hemoptysis'
    SEARCH_TRENDS_HEMORRHOIDS = 'search_trends_hemorrhoids'
    SEARCH_TRENDS_HEPATIC_ENCEPHALOPATHY = 'search_trends_hepatic_encephalopathy'
    SEARCH_TRENDS_HEPATITIS = 'search_trends_hepatitis'
    SEARCH_TRENDS_HEPATOTOXICITY = 'search_trends_hepatotoxicity'
    SEARCH_TRENDS_HICCUP = 'search_trends_hiccup'
    SEARCH_TRENDS_HIP_PAIN = 'search_trends_hip_pain'
    SEARCH_TRENDS_HIVES = 'search_trends_hives'
    SEARCH_TRENDS_HOT_FLASH = 'search_trends_hot_flash'
    SEARCH_TRENDS_HYDROCEPHALUS = 'search_trends_hydrocephalus'
    SEARCH_TRENDS_HYPERCALCAEMIA = 'search_trends_hypercalcaemia'
    SEARCH_TRENDS_HYPERCAPNIA = 'search_trends_hypercapnia'
    SEARCH_TRENDS_HYPERCHOLESTEROLEMIA = 'search_trends_hypercholesterolemia'
    SEARCH_TRENDS_HYPEREMESIS_GRAVIDARUM = 'search_trends_hyperemesis_gravidarum'
    SEARCH_TRENDS_HYPERGLYCEMIA = 'search_trends_hyperglycemia'
    SEARCH_TRENDS_HYPERHIDROSIS = 'search_trends_hyperhidrosis'
    SEARCH_TRENDS_HYPERKALEMIA = 'search_trends_hyperkalemia'
    SEARCH_TRENDS_HYPERLIPIDEMIA = 'search_trends_hyperlipidemia'
    SEARCH_TRENDS_HYPERMOBILITY = 'search_trends_hypermobility'
    SEARCH_TRENDS_HYPERPIGMENTATION = 'search_trends_hyperpigmentation'
    SEARCH_TRENDS_HYPERSOMNIA = 'search_trends_hypersomnia'
    SEARCH_TRENDS_HYPERTENSION = 'search_trends_hypertension'
    SEARCH_TRENDS_HYPERTHERMIA = 'search_trends_hyperthermia'
    SEARCH_TRENDS_HYPERTHYROIDISM = 'search_trends_hyperthyroidism'
    SEARCH_TRENDS_HYPERTRIGLYCERIDEMIA = 'search_trends_hypertriglyceridemia'
    SEARCH_TRENDS_HYPERTROPHY = 'search_trends_hypertrophy'
    SEARCH_TRENDS_HYPERVENTILATION = 'search_trends_hyperventilation'
    SEARCH_TRENDS_HYPOCALCAEMIA = 'search_trends_hypocalcaemia'
    SEARCH_TRENDS_HYPOCHONDRIASIS = 'search_trends_hypochondriasis'
    SEARCH_TRENDS_HYPOGLYCEMIA = 'search_trends_hypoglycemia'
    SEARCH_TRENDS_HYPOGONADISM = 'search_trends_hypogonadism'
    SEARCH_TRENDS_HYPOKALEMIA = 'search_trends_hypokalemia'
    SEARCH_TRENDS_HYPOMANIA = 'search_trends_hypomania'
    SEARCH_TRENDS_HYPONATREMIA = 'search_trends_hyponatremia'
    SEARCH_TRENDS_HYPOTENSION = 'search_trends_hypotension'
    SEARCH_TRENDS_HYPOTHYROIDISM = 'search_trends_hypothyroidism'
    SEARCH_TRENDS_HYPOXEMIA = 'search_trends_hypoxemia'
    SEARCH_TRENDS_HYPOXIA = 'search_trends_hypoxia'
    SEARCH_TRENDS_IMPETIGO = 'search_trends_impetigo'
    SEARCH_TRENDS_IMPLANTATION_BLEEDING = 'search_trends_implantation_bleeding'
    SEARCH_TRENDS_IMPULSIVITY = 'search_trends_impulsivity'
    SEARCH_TRENDS_INDIGESTION = 'search_trends_indigestion'
    SEARCH_TRENDS_INFECTION = 'search_trends_infection'
    SEARCH_TRENDS_INFLAMMATION = 'search_trends_inflammation'
    SEARCH_TRENDS_INFLAMMATORY_BOWEL_DISEASE = 'search_trends_inflammatory_bowel_disease'
    SEARCH_TRENDS_INGROWN_HAIR = 'search_trends_ingrown_hair'
    SEARCH_TRENDS_INSOMNIA = 'search_trends_insomnia'
    SEARCH_TRENDS_INSULIN_RESISTANCE = 'search_trends_insulin_resistance'
    SEARCH_TRENDS_INTERMENSTRUAL_BLEEDING = 'search_trends_intermenstrual_bleeding'
    SEARCH_TRENDS_INTRACRANIAL_PRESSURE = 'search_trends_intracranial_pressure'
    SEARCH_TRENDS_IRON_DEFICIENCY = 'search_trends_iron_deficiency'
    SEARCH_TRENDS_IRREGULAR_MENSTRUATION = 'search_trends_irregular_menstruation'
    SEARCH_TRENDS_ITCH = 'search_trends_itch'
    SEARCH_TRENDS_JAUNDICE = 'search_trends_jaundice'
    SEARCH_TRENDS_KIDNEY_FAILURE = 'search_trends_kidney_failure'
    SEARCH_TRENDS_KIDNEY_STONE = 'search_trends_kidney_stone'
    SEARCH_TRENDS_KNEE_PAIN = 'search_trends_knee_pain'
    SEARCH_TRENDS_KYPHOSIS = 'search_trends_kyphosis'
    SEARCH_TRENDS_LACTOSE_INTOLERANCE = 'search_trends_lactose_intolerance'
    SEARCH_TRENDS_LARYNGITIS = 'search_trends_laryngitis'
    SEARCH_TRENDS_LEG_CRAMPS = 'search_trends_leg_cramps'
    SEARCH_TRENDS_LESION = 'search_trends_lesion'
    SEARCH_TRENDS_LEUKORRHEA = 'search_trends_leukorrhea'
    SEARCH_TRENDS_LIGHTHEADEDNESS = 'search_trends_lightheadedness'
    SEARCH_TRENDS_LOW_BACK_PAIN = 'search_trends_low_back_pain'
    SEARCH_TRENDS_LOW_GRADE_FEVER = 'search_trends_low_grade_fever'
    SEARCH_TRENDS_LYMPHEDEMA = 'search_trends_lymphedema'
    SEARCH_TRENDS_MAJOR_DEPRESSIVE_DISORDER = 'search_trends_major_depressive_disorder'
    SEARCH_TRENDS_MALABSORPTION = 'search_trends_malabsorption'
    SEARCH_TRENDS_MALE_INFERTILITY = 'search_trends_male_infertility'
    SEARCH_TRENDS_MANIC_DISORDER = 'search_trends_manic_disorder'
    SEARCH_TRENDS_MELASMA = 'search_trends_melasma'
    SEARCH_TRENDS_MELENA = 'search_trends_melena'
    SEARCH_TRENDS_MENINGITIS = 'search_trends_meningitis'
    SEARCH_TRENDS_MENORRHAGIA = 'search_trends_menorrhagia'
    SEARCH_TRENDS_MIDDLE_BACK_PAIN = 'search_trends_middle_back_pain'
    SEARCH_TRENDS_MIGRAINE = 'search_trends_migraine'
    SEARCH_TRENDS_MILIUM = 'search_trends_milium'
    SEARCH_TRENDS_MITRAL_INSUFFICIENCY = 'search_trends_mitral_insufficiency'
    SEARCH_TRENDS_MOOD_DISORDER = 'search_trends_mood_disorder'
    SEARCH_TRENDS_MOOD_SWING = 'search_trends_mood_swing'
    SEARCH_TRENDS_MORNING_SICKNESS = 'search_trends_morning_sickness'
    SEARCH_TRENDS_MOTION_SICKNESS = 'search_trends_motion_sickness'
    SEARCH_TRENDS_MOUTH_ULCER = 'search_trends_mouth_ulcer'
    SEARCH_TRENDS_MUSCLE_ATROPHY = 'search_trends_muscle_atrophy'
    SEARCH_TRENDS_MUSCLE_WEAKNESS = 'search_trends_muscle_weakness'
    SEARCH_TRENDS_MYALGIA = 'search_trends_myalgia'
    SEARCH_TRENDS_MYDRIASIS = 'search_trends_mydriasis'
    SEARCH_TRENDS_MYOCARDIAL_INFARCTION = 'search_trends_myocardial_infarction'
    SEARCH_TRENDS_MYOCLONUS = 'search_trends_myoclonus'
    SEARCH_TRENDS_NASAL_CONGESTION = 'search_trends_nasal_congestion'
    SEARCH_TRENDS_NASAL_POLYP = 'search_trends_nasal_polyp'
    SEARCH_TRENDS_NAUSEA = 'search_trends_nausea'
    SEARCH_TRENDS_NECK_MASS = 'search_trends_neck_mass'
    SEARCH_TRENDS_NECK_PAIN = 'search_trends_neck_pain'
    SEARCH_TRENDS_NEONATAL_JAUNDICE = 'search_trends_neonatal_jaundice'
    SEARCH_TRENDS_NERVE_INJURY = 'search_trends_nerve_injury'
    SEARCH_TRENDS_NEURALGIA = 'search_trends_neuralgia'
    SEARCH_TRENDS_NEUTROPENIA = 'search_trends_neutropenia'
    SEARCH_TRENDS_NIGHT_SWEATS = 'search_trends_night_sweats'
    SEARCH_TRENDS_NIGHT_TERROR = 'search_trends_night_terror'
    SEARCH_TRENDS_NOCTURNAL_ENURESIS = 'search_trends_nocturnal_enuresis'
    SEARCH_TRENDS_NODULE = 'search_trends_nodule'
    SEARCH_TRENDS_NOSEBLEED = 'search_trends_nosebleed'
    SEARCH_TRENDS_NYSTAGMUS = 'search_trends_nystagmus'
    SEARCH_TRENDS_OBESITY = 'search_trends_obesity'
    SEARCH_TRENDS_ONYCHORRHEXIS = 'search_trends_onychorrhexis'
    SEARCH_TRENDS_ORAL_CANDIDIASIS = 'search_trends_oral_candidiasis'
    SEARCH_TRENDS_ORTHOSTATIC_HYPOTENSION = 'search_trends_orthostatic_hypotension'
    SEARCH_TRENDS_OSTEOPENIA = 'search_trends_osteopenia'
    SEARCH_TRENDS_OSTEOPHYTE = 'search_trends_osteophyte'
    SEARCH_TRENDS_OSTEOPOROSIS = 'search_trends_osteoporosis'
    SEARCH_TRENDS_OTITIS = 'search_trends_otitis'
    SEARCH_TRENDS_OTITIS_EXTERNA = 'search_trends_otitis_externa'
    SEARCH_TRENDS_OTITIS_MEDIA = 'search_trends_otitis_media'
    SEARCH_TRENDS_PAIN = 'search_trends_pain'
    SEARCH_TRENDS_PALPITATIONS = 'search_trends_palpitations'
    SEARCH_TRENDS_PANCREATITIS = 'search_trends_pancreatitis'
    SEARCH_TRENDS_PANIC_ATTACK = 'search_trends_panic_attack'
    SEARCH_TRENDS_PAPULE = 'search_trends_papule'
    SEARCH_TRENDS_PARANOIA = 'search_trends_paranoia'
    SEARCH_TRENDS_PARESTHESIA = 'search_trends_paresthesia'
    SEARCH_TRENDS_PELVIC_INFLAMMATORY_DISEASE = 'search_trends_pelvic_inflammatory_disease'
    SEARCH_TRENDS_PERICARDITIS = 'search_trends_pericarditis'
    SEARCH_TRENDS_PERIODONTAL_DISEASE = 'search_trends_periodontal_disease'
    SEARCH_TRENDS_PERIORBITAL_PUFFINESS = 'search_trends_periorbital_puffiness'
    SEARCH_TRENDS_PERIPHERAL_NEUROPATHY = 'search_trends_peripheral_neuropathy'
    SEARCH_TRENDS_PERSPIRATION = 'search_trends_perspiration'
    SEARCH_TRENDS_PETECHIA = 'search_trends_petechia'
    SEARCH_TRENDS_PHLEGM = 'search_trends_phlegm'
    SEARCH_TRENDS_PHOTODERMATITIS = 'search_trends_photodermatitis'
    SEARCH_TRENDS_PHOTOPHOBIA = 'search_trends_photophobia'
    SEARCH_TRENDS_PHOTOPSIA = 'search_trends_photopsia'
    SEARCH_TRENDS_PLEURAL_EFFUSION = 'search_trends_pleural_effusion'
    SEARCH_TRENDS_PLEURISY = 'search_trends_pleurisy'
    SEARCH_TRENDS_PNEUMONIA = 'search_trends_pneumonia'
    SEARCH_TRENDS_PODALGIA = 'search_trends_podalgia'
    SEARCH_TRENDS_POLYCYTHEMIA = 'search_trends_polycythemia'
    SEARCH_TRENDS_POLYDIPSIA = 'search_trends_polydipsia'
    SEARCH_TRENDS_POLYNEUROPATHY = 'search_trends_polyneuropathy'
    SEARCH_TRENDS_POLYURIA = 'search_trends_polyuria'
    SEARCH_TRENDS_POOR_POSTURE = 'search_trends_poor_posture'
    SEARCH_TRENDS_POST_NASAL_DRIP = 'search_trends_post_nasal_drip'
    SEARCH_TRENDS_POSTURAL_ORTHOSTATIC_TACHYCARDIA_SYNDROME = 'search_trends_postural_orthostatic_tachycardia_syndrome'
    SEARCH_TRENDS_PREDIABETES = 'search_trends_prediabetes'
    SEARCH_TRENDS_PROTEINURIA = 'search_trends_proteinuria'
    SEARCH_TRENDS_PRURITUS_ANI = 'search_trends_pruritus_ani'
    SEARCH_TRENDS_PSYCHOSIS = 'search_trends_psychosis'
    SEARCH_TRENDS_PTOSIS = 'search_trends_ptosis'
    SEARCH_TRENDS_PULMONARY_EDEMA = 'search_trends_pulmonary_edema'
    SEARCH_TRENDS_PULMONARY_HYPERTENSION = 'search_trends_pulmonary_hypertension'
    SEARCH_TRENDS_PURPURA = 'search_trends_purpura'
    SEARCH_TRENDS_PUS = 'search_trends_pus'
    SEARCH_TRENDS_PYELONEPHRITIS = 'search_trends_pyelonephritis'
    SEARCH_TRENDS_RADICULOPATHY = 'search_trends_radiculopathy'
    SEARCH_TRENDS_RECTAL_PAIN = 'search_trends_rectal_pain'
    SEARCH_TRENDS_RECTAL_PROLAPSE = 'search_trends_rectal_prolapse'
    SEARCH_TRENDS_RED_EYE = 'search_trends_red_eye'
    SEARCH_TRENDS_RENAL_COLIC = 'search_trends_renal_colic'
    SEARCH_TRENDS_RESTLESS_LEGS_SYNDROME = 'search_trends_restless_legs_syndrome'
    SEARCH_TRENDS_RHEUM = 'search_trends_rheum'
    SEARCH_TRENDS_RHINITIS = 'search_trends_rhinitis'
    SEARCH_TRENDS_RHINORRHEA = 'search_trends_rhinorrhea'
    SEARCH_TRENDS_ROSACEA = 'search_trends_rosacea'
    SEARCH_TRENDS_ROUND_LIGAMENT_PAIN = 'search_trends_round_ligament_pain'
    SEARCH_TRENDS_RUMINATION = 'search_trends_rumination'
    SEARCH_TRENDS_SCAR = 'search_trends_scar'
    SEARCH_TRENDS_SCIATICA = 'search_trends_sciatica'
    SEARCH_TRENDS_SCOLIOSIS = 'search_trends_scoliosis'
    SEARCH_TRENDS_SEBORRHEIC_DERMATITIS = 'search_trends_seborrheic_dermatitis'
    SEARCH_TRENDS_SELF_HARM = 'search_trends_self_harm'
    SEARCH_TRENDS_SENSITIVITY_TO_SOUND = 'search_trends_sensitivity_to_sound'
    SEARCH_TRENDS_SEXUAL_DYSFUNCTION = 'search_trends_sexual_dysfunction'
    SEARCH_TRENDS_SHALLOW_BREATHING = 'search_trends_shallow_breathing'
    SEARCH_TRENDS_SHARP_PAIN = 'search_trends_sharp_pain'
    SEARCH_TRENDS_SHIVERING = 'search_trends_shivering'
    SEARCH_TRENDS_SHORTNESS_OF_BREATH = 'search_trends_shortness_of_breath'
    SEARCH_TRENDS_SHYNESS = 'search_trends_shyness'
    SEARCH_TRENDS_SINUSITIS = 'search_trends_sinusitis'
    SEARCH_TRENDS_SKIN_CONDITION = 'search_trends_skin_condition'
    SEARCH_TRENDS_SKIN_RASH = 'search_trends_skin_rash'
    SEARCH_TRENDS_SKIN_TAG = 'search_trends_skin_tag'
    SEARCH_TRENDS_SKIN_ULCER = 'search_trends_skin_ulcer'
    SEARCH_TRENDS_SLEEP_APNEA = 'search_trends_sleep_apnea'
    SEARCH_TRENDS_SLEEP_DEPRIVATION = 'search_trends_sleep_deprivation'
    SEARCH_TRENDS_SLEEP_DISORDER = 'search_trends_sleep_disorder'
    SEARCH_TRENDS_SNORING = 'search_trends_snoring'
    SEARCH_TRENDS_SORE_THROAT = 'search_trends_sore_throat'
    SEARCH_TRENDS_SPASTICITY = 'search_trends_spasticity'
    SEARCH_TRENDS_SPLENOMEGALY = 'search_trends_splenomegaly'
    SEARCH_TRENDS_SPUTUM = 'search_trends_sputum'
    SEARCH_TRENDS_STOMACH_RUMBLE = 'search_trends_stomach_rumble'
    SEARCH_TRENDS_STRABISMUS = 'search_trends_strabismus'
    SEARCH_TRENDS_STRETCH_MARKS = 'search_trends_stretch_marks'
    SEARCH_TRENDS_STRIDOR = 'search_trends_stridor'
    SEARCH_TRENDS_STROKE = 'search_trends_stroke'
    SEARCH_TRENDS_STUTTERING = 'search_trends_stuttering'
    SEARCH_TRENDS_SUBDURAL_HEMATOMA = 'search_trends_subdural_hematoma'
    SEARCH_TRENDS_SUICIDAL_IDEATION = 'search_trends_suicidal_ideation'
    SEARCH_TRENDS_SWELLING = 'search_trends_swelling'
    SEARCH_TRENDS_SWOLLEN_FEET = 'search_trends_swollen_feet'
    SEARCH_TRENDS_SWOLLEN_LYMPH_NODES = 'search_trends_swollen_lymph_nodes'
    SEARCH_TRENDS_SYNCOPE = 'search_trends_syncope'
    SEARCH_TRENDS_TACHYCARDIA = 'search_trends_tachycardia'
    SEARCH_TRENDS_TACHYPNEA = 'search_trends_tachypnea'
    SEARCH_TRENDS_TELANGIECTASIA = 'search_trends_telangiectasia'
    SEARCH_TRENDS_TENDERNESS = 'search_trends_tenderness'
    SEARCH_TRENDS_TESTICULAR_PAIN = 'search_trends_testicular_pain'
    SEARCH_TRENDS_THROAT_IRRITATION = 'search_trends_throat_irritation'
    SEARCH_TRENDS_THROMBOCYTOPENIA = 'search_trends_thrombocytopenia'
    SEARCH_TRENDS_THYROID_NODULE = 'search_trends_thyroid_nodule'
    SEARCH_TRENDS_TIC = 'search_trends_tic'
    SEARCH_TRENDS_TINNITUS = 'search_trends_tinnitus'
    SEARCH_TRENDS_TONSILLITIS = 'search_trends_tonsillitis'
    SEARCH_TRENDS_TOOTHACHE = 'search_trends_toothache'
    SEARCH_TRENDS_TREMOR = 'search_trends_tremor'
    SEARCH_TRENDS_TRICHOPTILOSIS = 'search_trends_trichoptilosis'
    SEARCH_TRENDS_TUMOR = 'search_trends_tumor'
    SEARCH_TRENDS_TYPE_2_DIABETES = 'search_trends_type_2_diabetes'
    SEARCH_TRENDS_UNCONSCIOUSNESS = 'search_trends_unconsciousness'
    SEARCH_TRENDS_UNDERWEIGHT = 'search_trends_underweight'
    SEARCH_TRENDS_UPPER_RESPIRATORY_TRACT_INFECTION = 'search_trends_upper_respiratory_tract_infection'
    SEARCH_TRENDS_URETHRITIS = 'search_trends_urethritis'
    SEARCH_TRENDS_URINARY_INCONTINENCE = 'search_trends_urinary_incontinence'
    SEARCH_TRENDS_URINARY_TRACT_INFECTION = 'search_trends_urinary_tract_infection'
    SEARCH_TRENDS_URINARY_URGENCY = 'search_trends_urinary_urgency'
    SEARCH_TRENDS_UTERINE_CONTRACTION = 'search_trends_uterine_contraction'
    SEARCH_TRENDS_VAGINAL_BLEEDING = 'search_trends_vaginal_bleeding'
    SEARCH_TRENDS_VAGINAL_DISCHARGE = 'search_trends_vaginal_discharge'
    SEARCH_TRENDS_VAGINITIS = 'search_trends_vaginitis'
    SEARCH_TRENDS_VARICOSE_VEINS = 'search_trends_varicose_veins'
    SEARCH_TRENDS_VASCULITIS = 'search_trends_vasculitis'
    SEARCH_TRENDS_VENTRICULAR_FIBRILLATION = 'search_trends_ventricular_fibrillation'
    SEARCH_TRENDS_VENTRICULAR_TACHYCARDIA = 'search_trends_ventricular_tachycardia'
    SEARCH_TRENDS_VERTIGO = 'search_trends_vertigo'
    SEARCH_TRENDS_VIRAL_PNEUMONIA = 'search_trends_viral_pneumonia'
    SEARCH_TRENDS_VISUAL_ACUITY = 'search_trends_visual_acuity'
    SEARCH_TRENDS_VOMITING = 'search_trends_vomiting'
    SEARCH_TRENDS_WART = 'search_trends_wart'
    SEARCH_TRENDS_WATER_RETENTION = 'search_trends_water_retention'
    SEARCH_TRENDS_WEAKNESS = 'search_trends_weakness'
    SEARCH_TRENDS_WEIGHT_GAIN = 'search_trends_weight_gain'
    SEARCH_TRENDS_WHEEZE = 'search_trends_wheeze'
    SEARCH_TRENDS_XERODERMA = 'search_trends_xeroderma'
    SEARCH_TRENDS_XEROSTOMIA = 'search_trends_xerostomia'
    SEARCH_TRENDS_YAWN = 'search_trends_yawn'
    INGESTION_TIMESTAMP = 'ingestion_timestamp'


class colunasRenomeadasEnum(Enum):
    LOCATION_KEY = 'id_localizacao'
    DATE = 'data'
    INFANT_MORTALITY_RATE = 'taxa_mortalidade_infantil'
    ADULT_FEMALE_MORTALITY_RATE = 'taxa_mortalidade_adulta_feminina'
    ADULT_MALE_MORTALITY_RATE = 'taxa_mortalidade_adulta_masculina'
    POPULATION = 'populacao'
    POPULATION_AGE_00_09 = 'populacao_idade_00_09'
    POPULATION_AGE_10_19 = 'populacao_idade_10_19'
    POPULATION_AGE_20_29 = 'populacao_idade_20_29'
    POPULATION_AGE_30_39 = 'populacao_idade_30_39'
    POPULATION_AGE_40_49 = 'populacao_idade_40_49'
    POPULATION_AGE_50_59 = 'populacao_idade_50_59'
    POPULATION_AGE_60_69 = 'populacao_idade_60_69'
    POPULATION_AGE_70_79 = 'populacao_idade_70_79'
    POPULATION_AGE_80_AND_OLDER = 'populacao_idade_80_e_mais'
    POPULATION_FEMALE = 'populacao_feminina'
    POPULATION_MALE = 'populacao_masculina'
    POPULATION_RURAL = 'populacao_rural'
    POPULATION_URBAN = 'populacao_urbana'
    COUNTRY_CODE = 'codigo_pais'
    COUNTRY_NAME = 'nome_pais'
    CUMULATIVE_CONFIRMED = 'confirmados_acumulados'
    CUMULATIVE_DECEASED = 'falecidos_acumulados'
    CUMULATIVE_RECOVERED = 'recuperados_acumulados'
    CUMULATIVE_TESTED = 'testados_acumulados'
    SUBREGION1_CODE = 'codigo_subregiao1'
    SUBREGION1_NAME = 'nome_subregiao1'
    SUBREGION2_CODE = 'codigo_subregiao2'
    SUBREGION2_NAME = 'nome_subregiao2'
    INGESTION_TIMESTAMP = 'data_hora_ingestão'
    TOTAL_CASES = "total_casos"
    TOTAL_DEATHS = "total_mortes"
    TOTAL_RECOVERED = "total_recuperados"
    AVEREGE_CASES = "media_casos"
    AVEREGE_DEATHS = "media_mortes"
    AVEREGE_RECOVERED = "media_recuperados"

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2- Requisitos
# MAGIC
# MAGIC #### 2.1. Proponha uma arquitetura de dados para o pipeline de ETL:
# MAGIC
# MAGIC #####a. Explique como você planeja extrair os dados da fonte pública (URL).
# MAGIC
# MAGIC Resp.: Para extrair dados de um arquivo CSV disponível em uma URL pública usando a biblioteca requests e, em seguida, processá-lo com PySpark no Databricks, podemos seguir os seguintes passos para compor o pipeline de ETL. Nesse caso, requests fará o download do arquivo para o ambiente Databricks, e depois usaremos o PySpark para carregar os dados num dataframe. Conforme os CMD 6, 7 e 8.

# COMMAND ----------

# Método de ingestão de dados: baixar o arquivo da URL e salvá-lo na camada transient no DBFS.

def baixa_arquivos_url(url, file_path):
    response = requests.get(url)
    file_name = 'aggregated'
    
    # Ensure the directory exists
    full_path = f'/dbfs{file_path}'
    if not os.path.exists(full_path):
        os.makedirs(full_path)
    
    # Now construct the correct path for the file
    with open(f'{full_path}/{file_name}', 'wb') as file:
        file.write(response.content)
    return print("Arquivo CSV baixado com sucesso!")
    
 # O método lê os dados do data lake e gera um dataframe.
    
def transient_to_bronze(transient_path, bronze_path):
    print("=========================================================================")
    df = spark.read.csv(f'dbfs:{transient_path}', header=True, inferSchema=True)
    # Adiciona a coluna ingestion_timestamp, que representa o carimbo de data e hora de ingestão, ao DataFrame.
    data_hora_sao_paulo = spark.sql("SELECT to_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') as data_hora").collect()[0]['data_hora']
    df = df.withColumn(origemEnum.INGESTION_TIMESTAMP.value, lit(data_hora_sao_paulo))
    # Os dados são escritos na camada bronze do Data Lake.
    df.write.format(formato).mode(outputMode).save(bronze_path)
    print(f"{df.count()} arquivo ingerido com sucesso na camada bronze em formato Delta!")
    print("=========================================================================")
    return print(f"Arquivo removido com sucesso da camada transient: {dbutils.fs.rm(f'dbfs:{transient_path}', True)}")

# COMMAND ----------

# Processo de ingestão de dados da URL para o Data Lake.
baixa_arquivos_url(url, transient_path)
transient_to_bronze(transient_path, bronze_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### b. Descreva as etapas de transformação necessárias para preparar os dados para análise.
# MAGIC Resp.: As etapas são as seguintes:
# MAGIC - **Pré-análise dos dados originais**: é um processo que envolve a revisão e avaliação dos dados antes de qualquer transformação ou manipulação. Envolve a análise de schema, atributos e volumetria;
# MAGIC - **Limpeza e Validação**: envolve a remoção de registros duplicados, o tratamento de valores ausentes, quando necessário, e a aplicação da conversão de tipos de dados.
# MAGIC - **Enriquecimento de Dados**: Esta etapa contempla junções (embora não sejam aplicáveis neste caso), além da padronização e normalização dos dados.
# MAGIC - **Análise final dos dados**: Processo de realização de insights a partir dos dados.
# MAGIC

# COMMAND ----------

# DBTITLE 1,Relatório de Pré-análise dos Dados
def pre_analise(bronze_path):
    df = spark.read.format(formato).load(bronze_path)
    print("Pré-análise do dataset sobre a COVID-19")
    print("===========================================================================================")
    print("Detalhe do Schema de entrada do Dataset:")
    df.printSchema()
    print("===========================================================================================")
    print(f"A quantidade de atributos do DataFrame: {len(df.columns)}")
    print("===========================================================================================")
    print(f"A quantidade de linhas do DataFrame: {df.count()}")
    print("===========================================================================================")
    # Verificar se existem valores nulos
    num_nulls = df.select([col(column).isNull().cast("int").alias(column) for column in df.columns]).agg(*[sum(col(column)).alias(column) for column in df.columns]).collect()[0]
    # Exibir a quantidade de nulos por coluna
    print("Quantidade de valores nulos por coluna:")
    for column, count in zip(df.columns, num_nulls):
        print(f"{column}: {count}")
    print("===========================================================================================")

# COMMAND ----------

pre_analise(bronze_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### c. Explique como você irá carregar os dados transformados na plataforma Databricks.
# MAGIC Resp.: Utilizamos PySpark no Databricks para realizar transformações escaláveis em grandes volumes de dados armazenados em tabelas Delta, aproveitando a capacidade de distribuição em cluster.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.2. Implementação do Pipeline de ETL:
# MAGIC a. Implemente o processo de ETL utilizando Python/Databricks.<br>
# MAGIC b. Documente o código com comentários claros e concisos.<br>

# COMMAND ----------

# Devido à falta de documentação dos dados, interpretarei as taxas de mortalidade como taxas de mortalidade associadas à COVID-19.
# Tratamento de dados aplicados:
## 1. Seleciona os atributos;
## 2. Renomear os atributos que estão em ingles para portugues;
## 3. Transforma tipo date em timestamp;
## 3. Remover Acentos de uma Coluna;


lista_de_atributos = [origemEnum.LOCATION_KEY.value, 
                      origemEnum.DATE.value,
                      origemEnum.INFANT_MORTALITY_RATE.value,
                      origemEnum.ADULT_FEMALE_MORTALITY_RATE.value,
                      origemEnum.ADULT_MALE_MORTALITY_RATE.value,
                      origemEnum.POPULATION.value,
                      origemEnum.POPULATION_FEMALE.value,
                      origemEnum.POPULATION_MALE.value,
                      origemEnum.POPULATION_RURAL.value,
                      origemEnum.POPULATION_URBAN.value,
                      origemEnum.CUMULATIVE_CONFIRMED.value,
                      origemEnum.CUMULATIVE_DECEASED.value,
                      origemEnum.CUMULATIVE_RECOVERED.value,
                      origemEnum.CUMULATIVE_TESTED.value,
                      origemEnum.SUBREGION1_CODE.value,
                      origemEnum.SUBREGION1_NAME.value,
                      origemEnum.COUNTRY_CODE.value,
                      origemEnum.COUNTRY_NAME.value,
                      origemEnum.INGESTION_TIMESTAMP.value
                      ]
def tratamento(lt_atriburos: list, bronze_path, silver_path):
    df = spark.read.format(formato).load(bronze_path)
    df = df.select(lt_atriburos)\
        .withColumnRenamed(origemEnum.LOCATION_KEY.value, colunasRenomeadasEnum.LOCATION_KEY.value)\
        .withColumnRenamed(origemEnum.DATE.value, colunasRenomeadasEnum.DATE.value)\
        .withColumnRenamed(origemEnum.INFANT_MORTALITY_RATE.value, colunasRenomeadasEnum.INFANT_MORTALITY_RATE.value)\
        .withColumnRenamed(origemEnum.ADULT_FEMALE_MORTALITY_RATE.value, colunasRenomeadasEnum.ADULT_FEMALE_MORTALITY_RATE.value)\
        .withColumnRenamed(origemEnum.ADULT_MALE_MORTALITY_RATE.value, colunasRenomeadasEnum.ADULT_MALE_MORTALITY_RATE.value)\
        .withColumnRenamed(origemEnum.POPULATION.value, colunasRenomeadasEnum.POPULATION.value)\
        .withColumnRenamed(origemEnum.SUBREGION1_CODE.value, colunasRenomeadasEnum.SUBREGION1_CODE.value)\
        .withColumnRenamed(origemEnum.SUBREGION1_NAME.value, colunasRenomeadasEnum.SUBREGION1_NAME.value)\
        .withColumnRenamed(origemEnum.POPULATION_AGE_80_AND_OLDER.value, colunasRenomeadasEnum.POPULATION_AGE_80_AND_OLDER.value)\
        .withColumnRenamed(origemEnum.POPULATION_FEMALE.value, colunasRenomeadasEnum.POPULATION_FEMALE.value)\
        .withColumnRenamed(origemEnum.POPULATION_MALE.value, colunasRenomeadasEnum.POPULATION_MALE.value)\
        .withColumnRenamed(origemEnum.POPULATION_RURAL.value, colunasRenomeadasEnum.POPULATION_RURAL.value)\
        .withColumnRenamed(origemEnum.POPULATION_URBAN.value, colunasRenomeadasEnum.POPULATION_URBAN.value)\
        .withColumnRenamed(origemEnum.COUNTRY_CODE.value, colunasRenomeadasEnum.COUNTRY_CODE.value)\
        .withColumnRenamed(origemEnum.COUNTRY_NAME.value, colunasRenomeadasEnum.COUNTRY_NAME.value)\
        .withColumnRenamed(origemEnum.CUMULATIVE_CONFIRMED.value, colunasRenomeadasEnum.CUMULATIVE_CONFIRMED.value)\
        .withColumnRenamed(origemEnum.CUMULATIVE_DECEASED.value, colunasRenomeadasEnum.CUMULATIVE_DECEASED.value)\
        .withColumnRenamed(origemEnum.CUMULATIVE_RECOVERED.value, colunasRenomeadasEnum.CUMULATIVE_RECOVERED.value)\
        .withColumnRenamed(origemEnum.CUMULATIVE_TESTED.value, colunasRenomeadasEnum.CUMULATIVE_TESTED.value)\
        .withColumnRenamed(origemEnum.INGESTION_TIMESTAMP.value, colunasRenomeadasEnum.INGESTION_TIMESTAMP.value)
    def remover_acentos(texto):
        return unidecode(texto) if texto else None
    remover_acentos_udf = udf(remover_acentos, StringType())
    df = df.withColumn(colunasRenomeadasEnum.SUBREGION1_NAME.value, remover_acentos_udf(df[colunasRenomeadasEnum.SUBREGION1_NAME.value]))
    df.write.format(formato).mode(outputMode).save(silver_path)
    return print(f"{df.count()} linhas ingeridas com sucesso na camada silver!")

# COMMAND ----------

tratamento(lista_de_atributos, bronze_path, silver_path)

# COMMAND ----------

def processamento_gold(silver_path,gold_path):
    df = spark.read.format(formato).load(silver_path)
    df = df.groupBy(col(colunasRenomeadasEnum.COUNTRY_NAME.value))\
    .agg(sum(col(colunasRenomeadasEnum.CUMULATIVE_CONFIRMED.value)).alias(colunasRenomeadasEnum.TOTAL_CASES.value),
    sum(col(colunasRenomeadasEnum.CUMULATIVE_DECEASED.value)).alias(colunasRenomeadasEnum.TOTAL_DEATHS.value),
    sum(col(colunasRenomeadasEnum.CUMULATIVE_RECOVERED.value)).alias(colunasRenomeadasEnum.TOTAL_RECOVERED.value),
    round(avg(col(colunasRenomeadasEnum.CUMULATIVE_CONFIRMED.value)),2).alias(colunasRenomeadasEnum.AVEREGE_CASES.value),
    round(avg(col(colunasRenomeadasEnum.CUMULATIVE_DECEASED.value)),2).alias(colunasRenomeadasEnum.AVEREGE_DEATHS.value),
    round(avg(col(colunasRenomeadasEnum.CUMULATIVE_RECOVERED.value)),2).alias(colunasRenomeadasEnum.AVEREGE_RECOVERED.value))\
    .na.drop()\
    .orderBy(col(colunasRenomeadasEnum.COUNTRY_NAME.value))
    df.write.format(formato).mode(outputMode).save(gold_path)
    return print(f"{df.count()} linhas ingeridas com sucesso na camada gold!")

# COMMAND ----------

processamento_gold( silver_path, gold_path)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.3. Armazenamento na Plataforma Databricks:
# MAGIC ##### a. Escolha o formato de armazenamento adequado para os dados transformados
# MAGIC Resp.: Para uma estrutura de Data Lake o melhor formato é o Delta.
# MAGIC
# MAGIC #####  b. Justifique a escolha do formato de armazenamento e descreva como ele suporta consultas e análises eficientes.
# MAGIC Resp.: O formato Delta melhora o desempenho, confiabilidade e flexibilidade dos dados no Data Lake, facilitando análises rápidas e escaláveis ao permitir o processamento de dados complexos com garantia de consistência.

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.4. Análise Sumária dos Dados:
# MAGIC ##### a. Utilize a plataforma Databricks para realizar análises descritivas sobre os dados carregados.
# MAGIC ##### b. Gere visualizações e métricas chave (ex. número de casos, mortes, recuperações por região).
# MAGIC ##### c. Documente suas descobertas e insights derivados dos dados.

# COMMAND ----------

analitico_df = spark.read.format(formato).load(gold_path)

# COMMAND ----------

def relatorio_analitico(df: DataFrame):
  maior_total_casos = df.orderBy(df[colunasRenomeadasEnum.TOTAL_CASES.value].desc()).limit(1)
  menor_total_casos = df.orderBy(df[colunasRenomeadasEnum.TOTAL_CASES.value].asc()).limit(1)
  maior_total_mortes = df.orderBy(df[colunasRenomeadasEnum.TOTAL_DEATHS.value].desc()).limit(1)
  menor_total_mortes = df.orderBy(df[colunasRenomeadasEnum.TOTAL_DEATHS.value].asc()).limit(1)
  maior_total_recuperados = df.orderBy(df[colunasRenomeadasEnum.TOTAL_RECOVERED.value].desc()).limit(1)
  menor_total_recuperados = df.orderBy(df[colunasRenomeadasEnum.TOTAL_RECOVERED.value].asc()).limit(1)

  # Coletar o resultado e exibir com print
  print("Relatório Analítico sobre os Dados do COVID-19")
  print("=================================================================================================")
  resultado_01 = maior_total_casos.collect()[0]  # Pega a primeira linha
  resultado_02 = menor_total_casos.collect()[0]  # Pega a primeira linha
  resultado_03 = maior_total_mortes.collect()[0]  # Pega a primeira linha
  resultado_04 = menor_total_mortes.collect()[0]  # Pega a primeira linha
  resultado_05 = maior_total_recuperados.collect()[0]  # Pega a primeira linha
  resultado_06 = menor_total_recuperados.collect()[0]  # Pega a primeira linha
  print(f"País com maior quantidade de casos é: {resultado_01[colunasRenomeadasEnum.COUNTRY_NAME.value]} com {resultado_01[colunasRenomeadasEnum.TOTAL_CASES.value]} casos de COVID-19.")
  print(f"País com menor quantidade de casos é: {resultado_02[colunasRenomeadasEnum.COUNTRY_NAME.value]} com {resultado_02[colunasRenomeadasEnum.TOTAL_CASES.value]} casos de COVID-19.")
  print("=================================================================================================")
  print(f"País com maior quantidade de mostes é: {resultado_03[colunasRenomeadasEnum.COUNTRY_NAME.value]} com {resultado_03[colunasRenomeadasEnum.TOTAL_DEATHS.value]} casos de COVID-19.")
  print(f"País com menor quantidade de mostes é: {resultado_04[colunasRenomeadasEnum.COUNTRY_NAME.value]} com {resultado_04[colunasRenomeadasEnum.TOTAL_DEATHS.value]} casos de COVID-19.")
  print("=================================================================================================")
  print(f"País com maior quantidade de recuperados é: {resultado_05[colunasRenomeadasEnum.COUNTRY_NAME.value]} com {resultado_05[colunasRenomeadasEnum.TOTAL_RECOVERED.value]} casos de COVID-19.")
  print(f"País com menor quantidade de recuperados é: {resultado_06[colunasRenomeadasEnum.COUNTRY_NAME.value]} com {resultado_06[colunasRenomeadasEnum.TOTAL_RECOVERED.value]} casos de COVID-19.")
  print("=================================================================================================")
  media_casos = df.agg(avg(colunasRenomeadasEnum.TOTAL_CASES.value).alias("media_casos_geral")).collect()[0]["media_casos_geral"]
  print(f"Média Geral de casos de COVID-19: {media_casos:.0f}.")
  media_mortes = df.agg(avg(colunasRenomeadasEnum.TOTAL_DEATHS.value).alias("media_mortes_geral")).collect()[0]["media_mortes_geral"]
  print(f"Média Geral de mortes por COVID-19: {media_mortes:.0f}.")
  media_recuperados = df.agg(avg(colunasRenomeadasEnum.TOTAL_RECOVERED.value).alias("media_recuperados_geral")).collect()[0]["media_recuperados_geral"]
  print(f"Média Geral de recuperados do COVID-19: {media_recuperados:.0f}.")


# COMMAND ----------

relatorio_analitico(analitico_df)

# COMMAND ----------

def analise_grafica(df:DataFrame):
    # Agrega os totais
    df_totais = df.agg(
        sum(colunasRenomeadasEnum.TOTAL_CASES.value).alias(colunasRenomeadasEnum.TOTAL_CASES.value),
        sum(colunasRenomeadasEnum.TOTAL_DEATHS.value).alias(colunasRenomeadasEnum.TOTAL_DEATHS.value),
        sum(colunasRenomeadasEnum.TOTAL_RECOVERED.value).alias(colunasRenomeadasEnum.TOTAL_RECOVERED.value)
    )
    
    # Coleta os dados em um formato pandas DataFrame
    pandas_df = df.toPandas()
    totais = df_totais.collect()[0]

    # Configuração do subgráfico
    fig, axs = plt.subplots(2, 2, figsize=(16, 12), gridspec_kw={'height_ratios': [1, 1]})
    fig.suptitle('Análise de COVID-19 por País', fontsize=18)

    # Gráfico 1: Total de Casos
    pandas_df_sorted_cases = pandas_df.sort_values(by='total_casos', ascending=False)
    media_casos = pandas_df_sorted_cases['total_casos'].mean()
    bars_cases = axs[0, 0].barh(pandas_df_sorted_cases['nome_pais'], pandas_df_sorted_cases['total_casos'], color='skyblue', edgecolor='black')
    axs[0, 0].axvline(media_casos, color='red', linestyle='--', linewidth=2, label=f'Média: {media_casos:,.0f}')
    for bar in bars_cases:
        axs[0, 0].text(bar.get_width() + 3500000, bar.get_y() + bar.get_height() / 1.5,
                    f'{int(bar.get_width()):,}', va='center', ha='left', fontsize=10)
    axs[0, 0].set_title('Total de Casos de COVID-19 por País', fontsize=14)
    axs[0, 0].set_xlabel('Total de Casos', fontsize=12)
    axs[0, 0].invert_yaxis()
    axs[0, 0].grid(axis='x', linestyle='--', alpha=0.7)
    axs[0, 0].set_xticks([])  # Remove os valores do eixo x
    axs[0, 0].set_xlim(right=pandas_df_sorted_cases['total_casos'].max() * 1.2)  # Ajusta limite do eixo x
    axs[0, 0].legend(loc='lower right', fontsize=10)  # Adiciona legenda da média

    # Gráfico 2: Total de Mortes
    pandas_df_sorted_deaths = pandas_df.sort_values(by='total_mortes', ascending=False)
    media_deaths = pandas_df_sorted_deaths['total_mortes'].mean()
    bars_deaths = axs[0, 1].barh(pandas_df_sorted_deaths['nome_pais'], pandas_df_sorted_deaths['total_mortes'], color='salmon', edgecolor='black')
    axs[0, 1].axvline(media_deaths, color='red', linestyle='--', linewidth=2, label=f'Média: {media_deaths:,.0f}')
    for bar in bars_deaths:
        axs[0, 1].text(bar.get_width() + 20000, bar.get_y() + bar.get_height() / 2,
                    f'{int(bar.get_width()):,}', va='center', ha='left', fontsize=10)
    axs[0, 1].set_title('Total de Mortes de COVID-19 por País', fontsize=14)
    axs[0, 1].set_xlabel('Total de Mortes', fontsize=12)
    axs[0, 1].invert_yaxis()
    axs[0, 1].grid(axis='x', linestyle='--', alpha=0.7)
    axs[0, 1].set_xticks([])  # Remove os valores do eixo x
    axs[0, 1].set_xlim(right=pandas_df_sorted_deaths['total_mortes'].max() * 1.2)  # Ajusta limite do eixo x
    axs[0, 1].legend(loc='lower right', fontsize=10)  # Adiciona legenda da média

    # Gráfico 3: Total de Recuperados
    pandas_df_sorted_recovered = pandas_df.sort_values(by='total_recuperados', ascending=False)
    media_recovered = pandas_df_sorted_recovered['total_recuperados'].mean()
    bars_recovered = axs[1, 0].barh(pandas_df_sorted_recovered['nome_pais'], pandas_df_sorted_recovered['total_recuperados'], color='lightgreen', edgecolor='black')
    axs[1, 0].axvline(media_recovered, color='red', linestyle='--', linewidth=2, label=f'Média: {media_recovered:,.0f}')
    for bar in bars_recovered:
        axs[1, 0].text(bar.get_width() + 20000, bar.get_y() + bar.get_height() / 1.5,
                    f'{int(bar.get_width()):,}', va='center', ha='left', fontsize=10)
    axs[1, 0].set_title('Total de Recuperados de COVID-19 por País', fontsize=14)
    axs[1, 0].set_xlabel('Total de Recuperados', fontsize=12)
    axs[1, 0].invert_yaxis()
    axs[1, 0].grid(axis='x', linestyle='--', alpha=0.7)
    axs[1, 0].set_xticks([])  # Remove os valores do eixo x
    axs[1, 0].set_xlim(right=pandas_df_sorted_recovered['total_recuperados'].max() * 1.2)  # Ajusta limite do eixo x
    axs[1, 0].legend(loc='lower right', fontsize=10)  # Adiciona legenda da média

    # Gráfico 4: Gráfico de Pizza - Distribuição Mundial de Casos, Mortes e Recuperados
    labels = ['Total Casos', 'Total Mortes', 'Total Recuperados']
    sizes = [totais['total_casos'], totais['total_mortes'], totais['total_recuperados']]
    axs[1, 1].pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140, colors=['lightblue', 'salmon', 'lightgreen'])
    axs[1, 1].set_title('Distribuição Mundial de Casos, Mortes e Recuperados', fontsize=14)
    axs[1, 1].axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle

    # Ajusta layout
    plt.tight_layout(rect=[0, 0.03, 1, 0.95])  # Ajusta o layout para que o título não sobreponha os subgráficos
    plt.show()

# COMMAND ----------

analise_grafica(analitico_df)

# COMMAND ----------

def analise_correlacao(df:DataFrame):
    # Agrega os totais por país
    df_agregado = df.groupBy(colunasRenomeadasEnum.COUNTRY_NAME.value).agg(
        sum(colunasRenomeadasEnum.TOTAL_CASES.value).alias(colunasRenomeadasEnum.TOTAL_CASES.value),
        sum(colunasRenomeadasEnum.TOTAL_DEATHS.value).alias(colunasRenomeadasEnum.TOTAL_DEATHS.value),
        sum(colunasRenomeadasEnum.TOTAL_RECOVERED.value).alias(colunasRenomeadasEnum.TOTAL_RECOVERED.value))

    # Coleta os dados em um formato pandas DataFrame
    pandas_df = df_agregado.toPandas()

    # Calcula a correlação
    correlacao = pandas_df[[colunasRenomeadasEnum.TOTAL_CASES.value,
                            colunasRenomeadasEnum.TOTAL_DEATHS.value,
                            colunasRenomeadasEnum.TOTAL_RECOVERED.value]].corr()

    # Exibe a matriz de correlação
    print("Matriz de Correlação:")
    print(correlacao)

# COMMAND ----------

analise_correlacao(analitico_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Resultados Esperados:
# MAGIC A matriz de correlação mostrará os coeficientes de correlação entre total_casos, total_mortes e total_recuperados. 
# MAGIC a. Os valores variam de -1 a 1, onde:
# MAGIC - 1: indica uma correlação positiva perfeita,
# MAGIC - -1: indica uma correlação negativa perfeita,
# MAGIC - 0: indica nenhuma correlação.
# MAGIC #### Interpretação:
# MAGIC Você pode analisar como as variáveis estão relacionadas. Por exemplo, uma alta correlação entre total_casos e total_mortes pode indicar que mais casos estão associados a um maior número de mortes.

# COMMAND ----------

def relatorio_geral(df: DataFrame):
    df = df.groupBy(colunasRenomeadasEnum.COUNTRY_NAME.value).agg(
        sum(colunasRenomeadasEnum.TOTAL_CASES.value).alias(colunasRenomeadasEnum.TOTAL_CASES.value),
        sum(colunasRenomeadasEnum.TOTAL_DEATHS.value).alias(colunasRenomeadasEnum.TOTAL_DEATHS.value),
        sum(colunasRenomeadasEnum.TOTAL_RECOVERED.value).alias(colunasRenomeadasEnum.TOTAL_RECOVERED.value),
        avg(colunasRenomeadasEnum.AVEREGE_CASES.value).alias(colunasRenomeadasEnum.AVEREGE_CASES.value),
        avg(colunasRenomeadasEnum.AVEREGE_DEATHS.value).alias(colunasRenomeadasEnum.AVEREGE_DEATHS.value),
        avg(colunasRenomeadasEnum.AVEREGE_RECOVERED.value).alias(colunasRenomeadasEnum.AVEREGE_RECOVERED.value)
    )
    # Exibe as informações por país
    return df.show(truncate=False)

# COMMAND ----------

relatorio_geral(analitico_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.5. Implementação de Medidas de Segurança:
# MAGIC ##### a. Explique como você garante a segurança dos dados durante o processo de ETL.
# MAGIC Resp.: Uso de criptografia de dados, controle de acesso, Auditoria e Monitoramento,  Limpeza de Dados Sensíveis são boas praticas para garantir as medidas de segurança dos dados
# MAGIC ##### b. Descreva medidas de segurança implementadas para proteger os dados armazenados na plataforma Databricks.
# MAGIC Resp.: Seguem algumas medidas:
# MAGIC - Criptografia em Repouso: Databricks garante a criptografia em repouso para dados no Delta Lake e outros formatos, protegendo-os contra acessos físicos não autorizados;
# MAGIC - Controle de Acesso Granular:Utilize controle de acesso baseado em funções para restringir o acesso a notebooks, clusters e tabelas, definindo permissões específicas para leitura, escrita e execução em diferentes níveis de granularidade;
# MAGIC - Integração com IAM (Identity and Access Management): Integre o Databricks com serviços de gerenciamento de identidade, como AWS IAM ou Azure AD, para garantir uma autenticação e autorização seguras;
# MAGIC - Auditoria e Monitoramento de Atividades: Ative logs de auditoria para registrar acessos e operações em clusters e notebooks, permitindo a identificação de comportamentos suspeitos e fornecendo um registro para conformidade;
# MAGIC - Segurança da Rede: Implementar configurações de rede seguras, como VPCs e sub-redes, além de utilizar firewalls e grupos de segurança para restringir o acesso aos clusters Databricks.
# MAGIC - Proteção contra Perda de Dados: Configurar backups regulares e estratégias de recuperação de desastres para garantir que os dados possam ser restaurados em caso de perda ou corrupção.
# MAGIC
# MAGIC ##### Bibliografia
# MAGIC Databricks. (n.d.). Databricks Security Overview. Disponível em: [Databricks](https://docs.databricks.com/en/index.html).
# MAGIC
# MAGIC ##### Informação adicional
# MAGIC
# MAGIC A implementação dessas medidas de segurança durante o processo de ETL e no armazenamento de dados na plataforma Databricks não apenas protege dados sensíveis, mas também garante a conformidade com regulamentações importantes, como o GDPR e o HIPAA. É fundamental que a segurança seja uma prioridade em todas as etapas do ciclo de vida dos dados, desde a extração até a transformação e o armazenamento, assegurando a integridade e a confidencialidade das informações em todo o processo (NIST, 2020).
# MAGIC
# MAGIC Referência: NIST. (2020). Framework for Improving Critical Infrastructure Cybersecurity. National Institute of Standards and Technology. Disponível em: [NIST Cybersecurity Framework](https://www.nist.gov/cyberframework)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2.6. Testes Unitários:

# COMMAND ----------

# DBTITLE 1,teste transient_to_bronze
# 1. Importar as bibliotecas necessárias
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# 2. Criar uma sessão Spark
spark = SparkSession.builder \
    .appName("TestTransientToBronze") \
    .getOrCreate()

# 3. Criar um DataFrame de exemplo
data = [("exemplo1", 1), ("exemplo2", 2)]
columns = ["nome", "valor"]
test_df = spark.createDataFrame(data, columns)

# 4. Definir os caminhos para os dados de teste
transient_path = "/mnt/transient/test_data.csv"
bronze_path = "/mnt/bronze/test_data"

# 5. Salvar o DataFrame de exemplo na camada transitória
test_df.write.csv(f'dbfs:{transient_path}', header=True)

# 6. Função que você deseja testar
def transient_to_bronze(transient_path, bronze_path):
    df = spark.read.csv(f'dbfs:{transient_path}', header=True, inferSchema=True)
    data_hora_sao_paulo = spark.sql("SELECT to_utc_timestamp(current_timestamp(), 'America/Sao_Paulo') as data_hora").collect()[0]['data_hora']
    df = df.withColumn("ingestion_timestamp", lit(data_hora_sao_paulo))
    df.write.format("delta").mode("overwrite").save(bronze_path)
    print(f"{df.count()} arquivo ingerido com sucesso na camada bronze em formato Delta!")
    return dbutils.fs.rm(f'dbfs:{transient_path}', True)

# 7. Executar o método e verificar os resultados
transient_to_bronze(transient_path, bronze_path)

# 8. Verificar se os dados foram escritos na camada bronze
bronze_df = spark.read.format("delta").load(bronze_path)
print(f"Número de registros na camada bronze: {bronze_df.count()}")

# 9. Limpar os dados de teste (opcional)
dbutils.fs.rm(f'dbfs:{transient_path}', True)
dbutils.fs.rm(bronze_path, True)


# COMMAND ----------

# DBTITLE 1,teste-pre_analise
# 1. Criar um DataFrame de exemplo
data = [("exemplo1", 1, None), ("exemplo2", 2, 3), ("exemplo3", None, 5)]
columns = ["nome", "valor", "descricao"]
test_df = spark.createDataFrame(data, columns)

# 2. Caminho da camada bronze para testes
bronze_path = "/mnt/bronze/test_data"

# 3. Escrever o DataFrame na camada bronze
test_df.write.format("delta").mode("overwrite").save(bronze_path)

# 4. Chamar a função de pré-análise
pre_analise(bronze_path)

# 5. Limpar os dados de teste (opcional)
dbutils.fs.rm(bronze_path, True)


# COMMAND ----------

# DBTITLE 1,teste- tratamento
# Importações e inicialização
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from pyspark.sql.functions import udf
from unidecode import unidecode
from datetime import datetime

# Inicializa a sessão Spark
spark = SparkSession.builder \
    .appName("Test Tratamento") \
    .getOrCreate()

# Definição dos enums fictícios
class origemEnum:
    LOCATION_KEY = "LOCATION_KEY"
    DATE = "DATE"
    INFANT_MORTALITY_RATE = "INFANT_MORTALITY_RATE"
    ADULT_FEMALE_MORTALITY_RATE = "ADULT_FEMALE_MORTALITY_RATE"
    POPULATION = "POPULATION"
    SUBREGION1_NAME = "SUBREGION1_NAME"
    INGESTION_TIMESTAMP = "INGESTION_TIMESTAMP"

class colunasRenomeadasEnum:
    LOCATION_KEY = "location_key"
    DATE = "date"
    INFANT_MORTALITY_RATE = "infant_mortality_rate"
    ADULT_FEMALE_MORTALITY_RATE = "adult_female_mortality_rate"
    POPULATION = "population"
    SUBREGION1_NAME = "subregion1_name"
    INGESTION_TIMESTAMP = "ingestion_timestamp"

# Função de tratamento
def tratamento(df, lt_atriburos):
    df = df.select(lt_atriburos)\
        .withColumnRenamed(origemEnum.LOCATION_KEY, colunasRenomeadasEnum.LOCATION_KEY)\
        .withColumnRenamed(origemEnum.DATE, colunasRenomeadasEnum.DATE)\
        .withColumnRenamed(origemEnum.INFANT_MORTALITY_RATE, colunasRenomeadasEnum.INFANT_MORTALITY_RATE)\
        .withColumnRenamed(origemEnum.ADULT_FEMALE_MORTALITY_RATE, colunasRenomeadasEnum.ADULT_FEMALE_MORTALITY_RATE)\
        .withColumnRenamed(origemEnum.POPULATION, colunasRenomeadasEnum.POPULATION)\
        .withColumnRenamed(origemEnum.SUBREGION1_NAME, colunasRenomeadasEnum.SUBREGION1_NAME)\
        .withColumnRenamed(origemEnum.INGESTION_TIMESTAMP, colunasRenomeadasEnum.INGESTION_TIMESTAMP)

    def remover_acentos(texto):
        return unidecode(texto) if texto else None

    remover_acentos_udf = udf(remover_acentos, StringType())
    df = df.withColumn(colunasRenomeadasEnum.SUBREGION1_NAME, remover_acentos_udf(df[colunasRenomeadasEnum.SUBREGION1_NAME]))

    return df

# Célula 3: Teste da função de tratamento
# Criação de um DataFrame de exemplo
schema = StructType([
    StructField(origemEnum.LOCATION_KEY, StringType(), True),
    StructField(origemEnum.DATE, StringType(), True),
    StructField(origemEnum.INFANT_MORTALITY_RATE, FloatType(), True),
    StructField(origemEnum.ADULT_FEMALE_MORTALITY_RATE, FloatType(), True),
    StructField(origemEnum.POPULATION, FloatType(), True),
    StructField(origemEnum.SUBREGION1_NAME, StringType(), True),
    StructField(origemEnum.INGESTION_TIMESTAMP, TimestampType(), True)
])

data = [
    Row("LOC1", "2021-01-01", 5.0, 2.0, 1000.0, "São Paulo", datetime.now()),
    Row("LOC2", "2021-01-02", 4.0, 1.0, 1500.0, "Rio de Janeiro", datetime.now())
]

df_bronze = spark.createDataFrame(data, schema)

# Atributos a serem selecionados
lista_de_atributos = [
    origemEnum.LOCATION_KEY,
    origemEnum.DATE,
    origemEnum.INFANT_MORTALITY_RATE,
    origemEnum.ADULT_FEMALE_MORTALITY_RATE,
    origemEnum.POPULATION,
    origemEnum.SUBREGION1_NAME,
    origemEnum.INGESTION_TIMESTAMP
]

# Executando a função de tratamento
df_resultado = tratamento(df_bronze, lista_de_atributos)

# Verificações
assert df_resultado.count() == 2, "O número de linhas não está correto."
assert colunasRenomeadasEnum.LOCATION_KEY in df_resultado.columns, "Coluna LOCATION_KEY não renomeada corretamente."
assert df_resultado.filter(df_resultado[colunasRenomeadasEnum.SUBREGION1_NAME] == "Sao Paulo").count() == 1, "Acentos não foram removidos corretamente."

print("Todos os testes passaram com sucesso!")

# COMMAND ----------

# DBTITLE 1,teste-processamento_gold
# Célula 2: Importações e inicialização
from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, FloatType
from pyspark.sql.functions import col, sum, avg, round

# Inicializa a sessão Spark
spark = SparkSession.builder \
    .appName("Test Processamento Gold") \
    .getOrCreate()

# Definição dos enums fictícios
class colunasRenomeadasEnum:
    COUNTRY_NAME = "country_name"
    CUMULATIVE_CONFIRMED = "cumulative_confirmed"
    CUMULATIVE_DECEASED = "cumulative_deceased"
    CUMULATIVE_RECOVERED = "cumulative_recovered"
    TOTAL_CASES = "total_cases"
    TOTAL_DEATHS = "total_deaths"
    TOTAL_RECOVERED = "total_recovered"
    AVEREGE_CASES = "average_cases"
    AVEREGE_DEATHS = "average_deaths"
    AVEREGE_RECOVERED = "average_recovered"

formato = "delta"  # Defina seu formato, pode ser "delta", "parquet", etc.
outputMode = "overwrite"  # Ou "append", conforme necessário

# Função de processamento gold
def processamento_gold(silver_path, gold_path):
    df = spark.read.format(formato).load(silver_path)
    df = df.groupBy(col(colunasRenomeadasEnum.COUNTRY_NAME))\
        .agg(
            sum(col(colunasRenomeadasEnum.CUMULATIVE_CONFIRMED)).alias(colunasRenomeadasEnum.TOTAL_CASES),
            sum(col(colunasRenomeadasEnum.CUMULATIVE_DECEASED)).alias(colunasRenomeadasEnum.TOTAL_DEATHS),
            sum(col(colunasRenomeadasEnum.CUMULATIVE_RECOVERED)).alias(colunasRenomeadasEnum.TOTAL_RECOVERED),
            round(avg(col(colunasRenomeadasEnum.CUMULATIVE_CONFIRMED)), 2).alias(colunasRenomeadasEnum.AVEREGE_CASES),
            round(avg(col(colunasRenomeadasEnum.CUMULATIVE_DECEASED)), 2).alias(colunasRenomeadasEnum.AVEREGE_DEATHS),
            round(avg(col(colunasRenomeadasEnum.CUMULATIVE_RECOVERED)), 2).alias(colunasRenomeadasEnum.AVEREGE_RECOVERED)
        )\
        .na.drop()\
        .orderBy(col(colunasRenomeadasEnum.COUNTRY_NAME))
    
    df.write.format(formato).mode(outputMode).save(gold_path)
    return print(f"{df.count()} linhas ingeridas com sucesso na camada gold!")

# Célula 3: Teste da função de processamento gold
# Criação de um DataFrame de exemplo
schema = StructType([
    StructField(colunasRenomeadasEnum.COUNTRY_NAME, StringType(), True),
    StructField(colunasRenomeadasEnum.CUMULATIVE_CONFIRMED, FloatType(), True),
    StructField(colunasRenomeadasEnum.CUMULATIVE_DECEASED, FloatType(), True),
    StructField(colunasRenomeadasEnum.CUMULATIVE_RECOVERED, FloatType(), True)
])

data = [
    Row("Country A", 100.0, 10.0, 50.0),
    Row("Country A", 200.0, 20.0, 100.0),
    Row("Country B", 150.0, 15.0, 75.0)
]

# Criando DataFrame de exemplo no formato silver
silver_df = spark.createDataFrame(data, schema)
silver_path = "/tmp/silver_data"  # Ajuste o caminho conforme necessário
silver_df.write.format(formato).mode("overwrite").save(silver_path)

# Executando a função de processamento gold
gold_path = "/tmp/gold_data"  # Ajuste o caminho conforme necessário
processamento_gold(silver_path, gold_path)

# Verificando o resultado
gold_df = spark.read.format(formato).load(gold_path)
gold_df.show()

# Verificando as contagens
assert gold_df.count() == 2, "O número de linhas no DataFrame gold não está correto."
assert colunasRenomeadasEnum.TOTAL_CASES in gold_df.columns, "Coluna TOTAL_CASES não encontrada no DataFrame gold."
dbutils.fs.rm(silver_path, True)
dbutils.fs.rm(gold_path, True)

# COMMAND ----------


