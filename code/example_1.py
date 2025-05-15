# In this example, we will:
# 1. Create a study dataframe from the databricks dataframe
# 2. Filter the dataframe by NAICS_CODE
# 3. Process the VISITOR_HOME_CBGS column to get the total number of visitors
# 4. Plot the results

import pandas
import os
import matplotlib.pyplot as plt
import seaborn as sns

def print_split(x):
    print("-----------------------------------------------------------------------------")
    print(x)
    print("-----------------------------------------------------------------------------")


def wrapper(func, save_path, *args, **kwargs):
    if os.path.exists(save_path):
        print(f"Already exists: {save_path}")
        df = pandas.read_csv(save_path)
    else:
        print(f"Creating: {save_path}")
        df = func(*args, **kwargs)
        df.to_csv(save_path, index=False)
    print_split(df.head())
    return df




def get_study_df(databricks_df_path = 'data/atlanta.csv',STUDY_COLUMNS = ["LOCATION_NAME","PLACEKEY","POI_CBG", "NAICS_CODE","STREET_ADDRESS","POSTAL_CODE","VISITOR_HOME_CBGS","MEDIAN_DWELL","DISTANCE_FROM_HOME"]):
    databricks_df = pandas.read_csv(databricks_df_path)
    study_df = databricks_df[STUDY_COLUMNS]
    print_split(study_df.head())
    return study_df

def get_naics_df(input_df, naics_code):
    input_df['NAICS_CODE'] = input_df['NAICS_CODE'].astype(str)
    result_df = input_df[input_df['NAICS_CODE'].str.startswith(f'{naics_code}')]
    print_split(result_df.head())
    return result_df

def get_total_visitors(cell):
    if isinstance(cell, str):
        cell_as_dict = eval(cell)
        total_visitors = sum(cell_as_dict.values())
        return total_visitors     
    else:
        return 0

def get_total_visitors_df(input_df):
    visitors_df = input_df.copy()
    visitors_df['TOTAL_VISITORS'] = visitors_df['VISITOR_HOME_CBGS'].apply(get_total_visitors)
    print_split(visitors_df.head())
    return visitors_df

def plot(df, col1, col2):
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x=col1, y=col2)
    plt.title(f'Scatter plot of {col1} vs {col2}')
    plt.xlabel(col1)
    plt.ylabel(col2)
    plt.show()



def run_example_1():
    study_df = get_study_df(databricks_df_path='data/atlanta.csv')
    pharmacy_df = get_naics_df(input_df=study_df, naics_code='446110')
    visitors_df = get_total_visitors_df(input_df=pharmacy_df)
    plot(visitors_df, 'MEDIAN_DWELL', 'TOTAL_VISITORS')
    plot(visitors_df, 'POSTAL_CODE', 'TOTAL_VISITORS')
    plot(visitors_df, 'POSTAL_CODE', 'MEDIAN_DWELL')

def run_example_1_with_wrapper():
    os.makedirs('data/example1', exist_ok=True)
    study_df = wrapper(get_study_df, save_path='data/example1/study_atlanta.csv', databricks_df_path='data/atlanta.csv')
    pharmacy_df = wrapper(get_naics_df, save_path='data/example1/pharmacy.csv', input_df=study_df, naics_code='446110')
    visitors_df = wrapper(get_total_visitors_df, save_path='data/example1/total_visitors.csv', input_df=pharmacy_df)
    plot(visitors_df, 'MEDIAN_DWELL', 'TOTAL_VISITORS')
    plot(visitors_df, 'POSTAL_CODE', 'TOTAL_VISITORS')
    plot(visitors_df, 'POSTAL_CODE', 'MEDIAN_DWELL')
if __name__ == "__main__":
    # run_example_1()
    run_example_1_with_wrapper()