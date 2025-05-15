# In this example, we will:
# 1. Load the study dataframe from example_1
# 2. Clean the dataframe
# 3. Get the POI to CBG flows
# 4. Get the CBG to CBG flows
# 5. plot the results


import pandas
import os
import example_1


def print_split(x):
    print("-----------------------------------------------------------------------------")
    print(x)
    print("-----------------------------------------------------------------------------")

def get_cleaned_df(study_df):
    cleaned_df = study_df.copy()
    cleaned_df = cleaned_df.dropna(subset=['VISITOR_HOME_CBGS'])
    cleaned_df['VISITOR_HOME_CBGS'] = cleaned_df['VISITOR_HOME_CBGS'].apply(eval)
    print_split(cleaned_df.head())
    return cleaned_df

def get_poi_flows_df(flattened_df):
    poi_flows_df = flattened_df.copy()    
    poi_flows_df[['VISITOR_HOME_CBG', 'NUMBER_OF_VISITORS']] = pandas.DataFrame(poi_flows_df['VISITOR_HOME_CBGS'].tolist(), index=poi_flows_df.index)
    poi_flows_df.drop(columns=['VISITOR_HOME_CBGS'], inplace=True)
    print_split(poi_flows_df.head())
    return poi_flows_df

def get_flattened_df(cleaned_df):
    flattened_df = cleaned_df.copy()
    flattened_df['VISITOR_HOME_CBGS'] = flattened_df['VISITOR_HOME_CBGS'].apply(lambda d: list(d.items()))
    print_split(flattened_df.head())
    flattened_df = flattened_df.explode('VISITOR_HOME_CBGS')
    print_split(flattened_df.head())
    return flattened_df

def get_cbg_to_cbg_df(poi_flows_df):
    cbg_to_cbg_df = poi_flows_df.copy()
    cbg_to_cbg_df= cbg_to_cbg_df.groupby(['POI_CBG', 'VISITOR_HOME_CBG']).agg({'NUMBER_OF_VISITORS': 'sum'})
    cbg_to_cbg_df = cbg_to_cbg_df.reset_index()
    cbg_to_cbg_df = cbg_to_cbg_df.rename(columns={'POI_CBG': 'DESTINATION_CBG', 'VISITOR_HOME_CBG': 'ORIGIN_CBG'})
    print_split(cbg_to_cbg_df.head())
    return cbg_to_cbg_df

def run_example_2():
    study_df = example_1.get_study_df(STUDY_COLUMNS=["POI_CBG","LOCATION_NAME", "STREET_ADDRESS", "VISITOR_HOME_CBGS"])
    cleaned_df = get_cleaned_df(study_df)
    flatten_df = get_flattened_df(cleaned_df)
    poi_flows_df = get_poi_flows_df(flatten_df)
    poi_flows_df.to_csv('data/poi_flows_df.csv', index=False)
    cbg_to_cbg_df = get_cbg_to_cbg_df(poi_flows_df)
    print_info(study_df, cleaned_df, flatten_df, poi_flows_df, cbg_to_cbg_df)
    example_1.plot(poi_flows_df, 'POI_CBG', 'NUMBER_OF_VISITORS')
 

def print_info(study_df, cleaned_df, flatten_df, poi_flows_df, cbg_to_cbg_df):
    print("-----------------------------------------------------------------------------")
    print(f"{len(study_df)} rows in study_df")
    print(f"{len(cleaned_df)} rows in cleaned_df")
    print(f"{len(flatten_df)} rows in flatten_df")
    print(f"{len(poi_flows_df)} rows in poi_flows_df")
    print(f"{len(cbg_to_cbg_df)} rows in cbg_to_cbg_df")

def run_example_2_with_wrapper():
    os.makedirs('data/example2', exist_ok=True)
    study_df = example_1.wrapper(example_1.get_study_df, save_path='data/example2/study_atlanta.csv', STUDY_COLUMNS=["POI_CBG","LOCATION_NAME", "STREET_ADDRESS", "VISITOR_HOME_CBGS"])
    cleaned_df = example_1.wrapper(get_cleaned_df, save_path='data/example2/cleaned_df.csv', study_df=study_df)
    flatten_df = example_1.wrapper(get_flattened_df, save_path='data/example2/flatten_df.csv', cleaned_df=cleaned_df)
    poi_flows_df = example_1.wrapper(get_poi_flows_df, save_path='data/example2/poi_flows_df.csv', flattened_df=flatten_df)
    cbg_to_cbg_df = example_1.wrapper(get_cbg_to_cbg_df, save_path='data/example2/cbg_to_cbg_df.csv', poi_flows_df=poi_flows_df)
    print_info(study_df, cleaned_df, flatten_df, poi_flows_df, cbg_to_cbg_df)
    example_1.plot(poi_flows_df, 'POI_CBG', 'NUMBER_OF_VISITORS')


if __name__ == "__main__":
    # run_example_2()
    run_example_2_with_wrapper()