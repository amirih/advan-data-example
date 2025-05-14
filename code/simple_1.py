import pandas
import os


def get_small_df():
    if os.path.exists('data/small_atlanta.csv'):
        print("small_atlanta.csv already exists")
        small_df = pandas.read_csv('data/small_atlanta.csv')
    else:
        df = pandas.read_csv('data/atlanta.csv')
        small_df = df[["PLACEKEY","POI_CBG", "NAICS_CODE","STREET_ADDRESS","POSTAL_CODE","RAW_VISITOR_COUNTS","RAW_VISIT_COUNTS","VISITOR_HOME_CBGS","MEDIAN_DWELL","DISTANCE_FROM_HOME"]]
        small_df.to_csv('data/small_atlanta.csv', index=False)
    
    return small_df

def column_processor(column):
    if isinstance(column, float) and pandas.isna(column):
        return 0
    elif isinstance(column, str):
            column_dict = eval(column)
            total_visitors = sum(column_dict.values())
            return total_visitors     
    else:
        return -1
    
def main():
    small_df = get_small_df()
    small_df['VISITOR_HOME_CBGS_PROCESSED'] = small_df['VISITOR_HOME_CBGS'].apply(column_processor)
    print(small_df.head())


if __name__ == "__main__":
    main()