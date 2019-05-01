import numpy as np 
import pandas as pd
import functools
import inspect
import itertools
pd.options.display.max_colwidth = 100

# READING ALL THE SHEETS FROM EXCEL
xls = pd.ExcelFile('FI_Reference.xlsx')
sheets_dict = pd.read_excel(xls, sheet_name = [x for x in xls.sheet_names])

sheets_dict_name = {}
for key, value in sheets_dict.items():
    sheets_dict_name[f"df_{key}"] = value
print(sheets_dict_name.keys())
print()

xls2 = pd.ExcelFile('FI_Carrier_Rates_Benefits.xlsx')
sheets_dict2 = pd.read_excel(xls2, sheet_name = [x for x in xls2.sheet_names])

sheets_dict_carrier = {}
for key, value in sheets_dict2.items():
    sheets_dict_carrier[f"df_{key}"] = value
print(sheets_dict_carrier.keys())

# GLOBAL VALUES AND FUNCTIONS
class fixed_indemnity_global():

    def __init__(self, startingPeriodID, endingPeriodID, carrierID, carrierAreaGroupID, areaID):
        self.startingPeriodID = startingPeriodID
        self.endingPeriodID = endingPeriodID
        self.carrierID = carrierID
        self.carrierAreaGroupID = carrierAreaGroupID
        self.areaID = areaID
        
    def global_dict_df(self):
        dict_carrier = {}
        signature = inspect.getfullargspec(fixed_indemnity_global.__init__)
        args = signature.args
        
        for arg in args:
            if arg == 'self':
                pass
            elif arg == 'startingPeriodID':
                dict_carrier[arg] = self.startingPeriodID * len(self.carrierID)
            elif arg == 'endingPeriodID':
                dict_carrier[arg] = self.endingPeriodID * len(self.carrierID)
            elif arg == 'carrierID':
                dict_carrier[arg] = self.carrierID
            elif arg == 'carrierAreaGroupID':
                dict_carrier[arg] = self.carrierAreaGroupID * len(self.carrierID)
            elif arg == 'areaID':
                dict_carrier[arg] = self.areaID * len(self.carrierID)
        
        global_dict = dict_carrier
        global_df = pd.DataFrame(global_dict)

        return global_dict, global_df
fig = fixed_indemnity_global([241],[252], [x for x in range(7602,7637)],[1],[1])
period_area_dict, period_area_df = fig.global_dict_df()

# DECORATOR FUNCTION
def fixed_indemnity(func): 
    def wrapper_function(*args, **kwargs): 
        argspec = inspect.getfullargspec(func)
        len_argspec = (len(argspec.args))
        if '_' in func.__name__:
            name_split = func.__name__.split('_')
            if len_argspec > 1:
                for x in range(len_argspec):
                    print('{} dataframe was created.'.format(name_split[x].capitalize()))
                    print()
        else:
            print('{} dataframe was created.'.format(func.__name__.capitalize())) 
        print()
        return func(*args, **kwargs)    

    return wrapper_function

# CARRIER VIEW
@fixed_indemnity
def carriers(df_carriers, **kwargs):
    
    df_carriers_empty = df_carriers.iloc[0:0] 
    
    dict_kwarg = {}
    for name, value1 in kwargs.items():
        dict_kwarg[name] = value1
  
    def US_states():
        for kwarg, values in kwargs.items():
            if kwarg == 'US_states':
                StateLicensed = values
        return StateLicensed
    
    US_states = US_states()
    
    def update_dict_kwarg():
        for state in US_states:
            dict_kwarg['carrierDescription'].append(f"Limited Medical - {state}")
            dict_kwarg['StateLicensed'].append(state)
        return dict_kwarg
    
    updated_dict = update_dict_kwarg()
    
    def combine_dict():
        
        dict_copy = updated_dict.copy()   
        dict_copy.update(period_area_dict)   
        return dict_copy
    
    combine_dict = combine_dict()
    df_combine_dict = pd.DataFrame(dict([(key,pd.Series(value)) for key, value in combine_dict.items()]))
    
    df_carriers = pd.concat([df_carriers_empty, df_combine_dict], sort = False).fillna(method = 'ffill')
    df_carriers.drop(['carrierAreaGroupID', 'areaID','US_states'], axis = 1, inplace = True)
    return df_carriers

df_carrier = carriers(sheets_dict_name['df_carriers'], carrierName = 'HII - Cardinal Choice', 
                       carrierDescription = [], carrierLogoFile = 'hiiq', 
                       insuranceTypeID = 'E',CarrierCode = [], RateSetCsv = 'Current,Renewal', StateLicensed = [], 
                      US_states = ['AL','AZ','CA','DE','DC','FL','GA','HI','IA','IL','IN','KY','LA','MO','MS',
                                  'NC','NE','NM','NV', 'OH','OK','OR','PA','RI','SC','TN','TX','UT','VA','WI',
                                   'WV','WY','AK','MI','ND'])

# CAGS AND AREAS VIEW
@fixed_indemnity
def cags_areas(cags, areas):

    df_cag_empty = cags.iloc[0:0]
    df_cags = pd.concat([df_cag_empty, period_area_df], sort=False).fillna('')
    df_cags.drop(['areaID'], axis = 1, inplace = True)
    
    df_area_empty = areas.iloc[0:0]
    df_areas = pd.concat([df_area_empty, period_area_df], sort=False).fillna('')
        
    return df_cags, df_areas

df_cags, df_areas = cags_areas(sheets_dict_name['df_cags'],sheets_dict_name['df_areas'])

# Counties View
@fixed_indemnity
def counties(df_counties,county_list):
   
    df_dict = {}
    df_dict['df_counties'] = df_counties.iloc[0:0]
    for key, value in df_dict.items():
        df_county = pd.concat([value, period_area_df], sort = False)
        df_county = df_county.merge(df_carrier)
        df_county = df_county.iloc[:,:6]
        df_county['StateLicensed'] = df_carrier['StateLicensed'].values

        countyID_stateabbreviation = county_list[['countyID', 'stateAbbreviation']]
        countyID_stateabbreviation_dict = countyID_stateabbreviation.to_dict()
        
        countyID_list = []
        stateAbbreviation_list = []
        for all_key, all_value in countyID_stateabbreviation_dict.items():
            if all_key == 'countyID':
                for county_key, county_value in all_value.items():
                    countyID_list.extend([county_value])
            elif all_key == 'stateAbbreviation':
                for state_key, state_value in all_value.items():
                    stateAbbreviation_list.extend([state_value])
            else:
                pass
        
        zipped_list = list(zip(stateAbbreviation_list,countyID_list))
        compressed_dict = {'countyID_state': zipped_list}
        
        state_licensed = df_carrier['StateLicensed']
        edited_countyID_list = []
        edited_stateAbbreviation_list = []
        for carrier_key, carrier_state in state_licensed.items():
            for compressed_key, compressed_value in compressed_dict.items():
                for x in range(len(compressed_dict['countyID_state'])):
                    if compressed_value[x][0] == carrier_state:
                        edited_countyID_list.extend([compressed_value[x][1]])
                        edited_stateAbbreviation_list.extend([compressed_value[x][0]])
       
        edited_countyID_state_dict = {'countyID':edited_countyID_list, 'stateAbbreviation':edited_stateAbbreviation_list}
        df_countyID_state = pd.DataFrame(edited_countyID_state_dict)
        
            
        df_counties = df_county.merge(df_countyID_state, how = 'outer', left_on = 'StateLicensed', 
                                      right_on = 'stateAbbreviation',sort = False).drop(columns = ['countyID_x',
                                                                'StateLicensed', 'stateAbbreviation']).rename(columns={
                                                                'countyID_y':'countyID'})
        sort_columns = ['startingPeriodID', 'endingPeriodID', 'carrierID', 'carrierAreaGroupID', 'countyID', 'areaID']
        df_counties = df_counties[sort_columns]
       
        return df_counties

df_counties = counties(sheets_dict_name['df_counties'],sheets_dict_name['df_countylist'])

#Plan View
@fixed_indemnity
def plans(df_plans, planID, **kwargs):
    
    df_plans_empty = df_plans.iloc[0:0]
    df_plans = pd.concat([df_plans_empty, period_area_df], sort=False).fillna('').drop(['areaID'], axis = 1)
    df_plans['StateLicensed'] = df_carrier['StateLicensed'].values 
    
    planIDs = []
    for state in df_plans['StateLicensed']:
        for planname in planID:
            planIDs.extend([state+planname])
    planIDs = np.array(planIDs)
    
    for kwarg, value in kwargs.items():
        if kwarg == 'planName':
            planName = np.array(value)
            planName = np.tile(planName, len(df_carrier['StateLicensed'].values))
        elif kwarg == 'planState':
            planState = np.repeat(df_carrier['StateLicensed'].values, len(np.unique(planName)))
        elif kwarg == 'planTypeID':
            planTypeID = np.repeat(value, len(planName))
        elif kwarg == 'insuranceTypeID':
            insuranceTypeID = np.repeat(value,len(planName))
        else:
            ratingMethodID = np.repeat(value, len(planName))
    
    kwarg_dict = {'planID':planIDs,'planName':planName,'planState':planState,'insuranceTypeID':insuranceTypeID,
                  'planTypeID':planTypeID,'ratingMethodID':ratingMethodID}
    df_kwargs = pd.DataFrame(kwarg_dict)
    df_plans = pd.concat([period_area_df, df_plans_empty],sort=False).fillna(method = 'ffill').reset_index().drop(
                columns = ['areaID', 'index'])
    df_plans['states'] = df_carrier['StateLicensed'].values
    df_plans = df_plans.merge(df_kwargs, how = 'outer', left_on = 'states', right_on = 'planState', sort = False)
    df_plans = df_plans[['startingPeriodID', 'endingPeriodID', 'carrierID', 'planID_y', 'carrierAreaGroupID', 'planName_y',
                        'planTypeID_y', 'insuranceTypeID_y', 'planState_y', 'ratingMethodID_y']]
    
    column_list = list(df_plans.columns)
    column_list_replace = []
    for ind, column in enumerate(column_list):
        if '_y' in column_list[ind]:
            column = column.replace('_y','')
            column_list_replace.append(column)
        else:
            column_list_replace.append(column)
    
    df_plans.columns = column_list_replace
            
    return df_plans

df_plans = plans(sheets_dict_name['df_plans'], 
      ['CCE0' + str(x) for x in range(1,9)],#PLAN ID              
     planName = ['Plan'+ ' ' + str(x) for x in range(1,9)],
      planTypeID = 'IND',
     insuranceTypeID = 'E',
     planState = 'AL',
     ratingMethodID = 1)

#Rates View
@fixed_indemnity
def rates(df_rates, df_carrier_rates, planname, ageheader, num_of_rates_per_plan, num_of_coverageType, **kwargs):
    
    # Only include up to planName.
    drop_plan_columns = (np.array(df_plans.columns[6:]))
    df_new_plan = df_plans.drop(columns = drop_plan_columns) 
    df_rates = df_rates.merge(df_new_plan, how = 'outer', on = ['planID', 'startingPeriodID',
                                                        'endingPeriodID','carrierID']).drop(index = [0]).reset_index().drop(
                                                            columns = 'index')
    
    # Only include up to areaID.
    drop_area_columns = np.array(df_areas.columns[5:]) 
    df_new_areas = df_areas.drop(columns = drop_area_columns) 
    df_rates = df_rates.merge(df_new_areas, how = 'outer', on = ['areaID', 'carrierAreaGroupID', 'startingPeriodID',
                                                             'endingPeriodID','carrierID']).fillna(method = 'bfill')
    df_rates = df_rates.iloc[:-len(df_carrier['StateLicensed'])]
    
    # Specify in the argument the name of the plans from carrier rateslist
    plans = []
    for column in df_carrier_rates.columns:
        if planname in column:
            plans.append(column)
    plans = np.array(plans)
    
     # Specify the column name of the 'age header'.
    age_min = []
    age_max = []
    for value in df_carrier_rates[ageheader]:
        if '-' in value:
            value = value.replace('-', ' ').split(' ')
            age_min.append(value[0])
            age_max.append(value[1])
        else:
            age_min.append(value[0])
            age_max.append(value[1])
    age_min = np.array(age_min)
    age_max = np.array(age_max)
    
    # Get rid of the Plan Name and Age Header columns from carrier rateslist.
    drop_columns = []
    for columns in df_carrier_rates.columns:
        if planname in columns or ageheader in columns:
            drop_columns.append(columns)
    df_rateslist_clean = df_carrier_rates.drop(columns = drop_columns).unstack()
    
    # A cleaned data, num of plans reflects the total number of coverageType.
    # Num of rates per plan reflects the total rates per PLAN.
    df_rateslist_clean = pd.DataFrame(df_rateslist_clean).reset_index().rename(columns={'level_0':'coverageTypeID', 0:'rate'}).drop(
                                            columns = 'level_1').set_index(plans.repeat(
                                            num_of_rates_per_plan))
    df_rateslist_clean['rateMinAge'] = np.tile(age_min, num_of_coverageType)
    df_rateslist_clean['rateMaxAge'] = np.tile(age_max, num_of_coverageType)
    df_rateslist_clean = df_rateslist_clean.round(2).reset_index().rename(columns = {'index':'planName'})
    
    # Cleaning the coverageTypeID decimals that are not needed.
    rstrip_string = ''
    for num in range(1,len(df_rateslist_clean['planName'].unique())):
        rstrip_string += f".{num}"
    df_rateslist_clean['coverageTypeID'] = df_rateslist_clean['coverageTypeID'].map(lambda x: x.rstrip(rstrip_string))
    
    # A dictionary that has the corresponding plan name to each planID.
    dict_plan = {}
    for value in df_rates['planID']:
        for value2 in df_rates['planName']:
            for num in range(1,(len(df_plans['planName'])+1)):
                if str(num) in value and str(num) in value2:
                    dict_plan[value] = value2
    
    # A list for MultiIndex tuple for next block of code.
    newlist = []
    for value in df_rateslist_clean['planName'].values:
        for key, value2 in dict_plan.items():
            if value2 == value:
                newlist.append(key)
    
    # Make a dataframe with multiplied data arrays by the num of states.
    rateslist_dict = {}
    for column in df_rateslist_clean.columns:
        rateslist_dict[f"{column}"]= np.repeat(df_rateslist_clean[column], len(df_carrier['StateLicensed']))
    df_repeated_rateslist = pd.DataFrame(rateslist_dict, columns = df_rateslist_clean.columns)
    df_repeated_rateslist['planID'] = newlist
    df_rate_paste = (df_rates.astype(str).merge(df_repeated_rateslist.astype(str), on = [
                    'planID','planName'], how= 'outer',sort=True))
   
    for column in df_rate_paste.columns:
        if '_x' in column:
            df_rate_paste.drop([column], axis = 1, inplace = True)
        elif '_y' in column:
            df_rate_paste.rename(columns = {column:column.replace('_y','')}, inplace = True)
        else:
            pass
    
    df_rate_paste.drop(columns = ['carrierAreaGroupID', 'planName'], inplace = True)

    column_to_areaID = list(df_rate_paste.columns[:5])
    column_coverageTypeID = list([df_rate_paste.columns[12]])
    column_to_rateGender = list(df_rate_paste.columns[5:7])
    column_to_rateMaxAge = list(df_rate_paste.columns[14:])
    column_rate = list([df_rate_paste.columns[13]])
    column_to_paymentOption = list(df_rate_paste.columns[7:9])
    column_rateUnitID = list([df_rate_paste.columns[9]])
    column_to_maxUnit = list(df_rate_paste.columns[10:12])
    
    merged_column = column_to_areaID + column_coverageTypeID + column_rate + column_to_rateMaxAge + column_to_rateGender + column_to_paymentOption + column_rateUnitID + column_to_maxUnit
    df_rate_paste = df_rate_paste[merged_column]
    
    return df_rate_paste
                
rates_info = rates(sheets_dict_name['df_rates'],sheets_dict_carrier['df_rateslist_CA_OH'], 'Plan', 'Age Band', 40, 32)

#Benefits View
@fixed_indemnity
def benefits(df_benefits_view,df_benefitsID_view, df_benefitsCarrier_view):
    
    df_plans_truncated = df_plans[['startingPeriodID','endingPeriodID','carrierID','planID', 'planName', 'planTypeID']]
    df_benefits_empty = df_benefits_view.iloc[0:0]
    df_benefits = pd.concat([df_plans_truncated, df_benefits_empty], sort=False).fillna('').drop(columns = [
        'benefitID', 'benefitName', 'benefitDesc', 'tinyDescription', 'coverageTypeID', 'coverageTypeDesc'
    ])

    df_benefitsID = df_benefitsID_view[['benefitID', 'benefitName']]
    df_ID_Carrier = df_benefitsID.merge(df_benefitsCarrier_view, on = 'benefitName', sort = True)
    df_benefits_info = df_benefits.merge(df_ID_Carrier, on = ['planName'])
    
    startingPeriod_medigapID = list(df_benefits_info.columns[:7])
    benefitID_coverageDesc = list(df_benefits_info.columns[-6:-4]) + list(df_benefits_info.columns[-2:])
    serviceTypeID_cost = list(df_benefits_info.columns[7:15])
    trailingSpace_comments = list(df_benefits_info.columns[15:20])
    
    df_benefits_info = df_benefits_info[startingPeriod_medigapID + benefitID_coverageDesc + serviceTypeID_cost + 
                                        ['benefitDesc'] + trailingSpace_comments +['tinyDescription']]
    df_benefits_info[['benefitDesc', 'tinyDescription']] = df_benefits_info[['benefitDesc', 'tinyDescription']].fillna('N/A')
    df_benefits_info[['addTrailingSpace','addTrailingSpaceShort']] = False
    
    return df_benefits_info



# ALL THE VALUES ARE PLACED HERE!!!
fig = fixed_indemnity_global([241],[252], [x for x in range(7602,7637)],[1],[1])
period_area_dict, period_area_df = fig.global_dict_df()

df_carrier = carriers(sheets_dict_name['df_carriers'], carrierName = 'HII - Cardinal Choice', 
                       carrierDescription = [], carrierLogoFile = 'hiiq', 
                       insuranceTypeID = 'E',CarrierCode = [], RateSetCsv = 'Current,Renewal', StateLicensed = [], 
                      US_states = ['AL','AZ','CA','DE','DC','FL','GA','HI','IA','IL','IN','KY','LA','MO','MS',
                                  'NC','NE','NM','NV', 'OH','OK','OR','PA','RI','SC','TN','TX','UT','VA','WI',
                                   'WV','WY','AK','MI','ND'])

df_cags, df_areas = cags_areas(sheets_dict_name['df_cags'],sheets_dict_name['df_areas'])
df_counties = counties(sheets_dict_name['df_counties'],sheets_dict_name['df_countylist'])

df_plans = plans(sheets_dict_name['df_plans'], 
      ['CCE0' + str(x) for x in range(1,9)],#PLAN ID              
     planName = ['Plan'+ ' ' + str(x) for x in range(1,9)],
      planTypeID = 'IND',
     insuranceTypeID = 'E',
     planState = 'AL',
     ratingMethodID = 1)

rates_info = rates(sheets_dict_name['df_rates'],sheets_dict_carrier['df_rateslist_CA_OH'], 'Plan', 'Age Band', 40, 32)

benefits_info = benefits(sheets_dict_name['df_benefits'], sheets_dict_name['df_benefitIDs'],sheets_dict_carrier[
                                                                                    'df_benefitscarrier'])


# WRITING THE FINISHED DATAFRAME INTO EXCEL
writer = pd.ExcelWriter('Cardinal_Choice_Build.xlsx', engine = 'xlsxwriter')
keys = [x for x in xls.sheet_names[:9]]
values = [df_carrier, df_cags, df_areas,df_zipcodes, df_zipranges, df_counties, df_plans, rates_info, benefits_info]
df_dictionary = dict(zip(keys, values))
for sheet, df in df_dictionary.items(): 
    df.to_excel(writer, sheet_name = sheet)

#critical last step
writer.save()
