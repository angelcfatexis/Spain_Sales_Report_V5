import pandas as pd
import polars as pl
import time
import os

###################################### POLARS ################################

'''
def conversion_to_polars_file1(data: pd.DataFrame) -> pl.DataFrame:
   # Comentario explicativo de la funcion
   
    return pl.from_pandas(data)


def conversion_to_polars_file2(data: pd.DataFrame) -> pl.DataFrame:
   # Comentario explicativo de la funcion
   
    return pl.from_pandas(data)
'''

def conversion_to_polars_file1() -> pl.DataFrame:
# Comentario explicativo de la funcion


    return pl.read_csv('data/01_raw/male_players_1million_2_simplified.csv', infer_schema_length= 10000)


def conversion_to_polars_file2() -> pl.DataFrame:
   # Comentario explicativo de la funcion
   
    return pl.read_csv('data/01_raw/male_players_1million_1_simplified.csv', infer_schema_length= 10000)



def filter_overall_over_60_file1(data: pl.DataFrame) -> pl.DataFrame:
# Comentario explicativo de la funcion


    global start_time
    
    start_time = time.time()
    
    return data.filter(pl.col('overall') > 60)

def sort_by_short_name_file1(data: pl.DataFrame) -> pl.DataFrame:
   # Comentario explicativo de la funcion
   
    return data.sort("short_name")


def sort_by_potential_file2(data: pl.DataFrame) -> pl.DataFrame:
   # Comentario explicativo de la funcion
   
    return data.sort("potential")

def creation_of_cross_file2(data: pl.DataFrame) -> pl.DataFrame:
   # Comentario explicativo de la funcion
   
    df_return = data.with_columns(
        [
            (pl.col('overall')*pl.col('potential')).alias('cross'),
            (pl.col('potential')/pl.col('overall')).alias('cross2'),
            (pl.col('overall') + pl.col('potential')).alias('cross3'),
        ]
    )
    return df_return


def groupby_short_name_file2(data: pl.DataFrame) -> pl.DataFrame:
# Comentario explicativo de la funcion


    df_return = (
    data
    .groupby(["short_name"])
    .agg(
        [
            pl.col("cross").mean(),
            pl.col('cross2').mean(),
            pl.col('cross3').mean()
        ]
    )
)
    return df_return


def inner_join_over_short_name(data_left: pl.DataFrame,
                               data_right: pl.DataFrame) -> pl.DataFrame:
    
    # Comentario explicativo de la funcion
    return data_left.join(data_right, on="short_name", how="inner")

def filter_potential_over_66_file1(data: pl.DataFrame) -> pl.DataFrame:
    # Comentario explicativo de la funcion
    return data.filter(pl.col('potential') > 66)

def group_by_shor_tname_joined_data(data: pl.DataFrame) -> pl.DataFrame:
   # Comentario explicativo de la funcion
   
    data = (data
            .groupby(["short_name"])
            .agg(
                [
                    pl.col("overall").mean(),
                    pl.col('potential').mean(),
                    pl.col('cross').mean(),
                    pl.col('cross2').mean(),
                    pl.col('cross3').mean()
                ]
            )
        )
    
    return data

def sort_by_short_name_joined_data(data: pl.DataFrame) -> pl.DataFrame:
# Comentario explicativo de la funcion


    
    data_to_return = data.sort('short_name')
    print(time.time()-start_time)
    print(data_to_return.head(4))
    return data_to_return




###################################### POLARS LAZYFRAME  ################################

'''

def conversion_to_polars_file1() -> pl.LazyFrame:
# Comentario explicativo de la funcion


    return pl.scan_csv('data/01_raw/male_players_1million_2_simplified.csv', infer_schema_length= 10000)


def conversion_to_polars_file2() -> pl.LazyFrame:
   # Comentario explicativo de la funcion
   
    return pl.scan_csv('data/01_raw/male_players_1million_1_simplified.csv', infer_schema_length= 10000)



def filter_overall_over_60_file1(data: pl.LazyFrame) -> pl.LazyFrame:
# Comentario explicativo de la funcion


    global start_time
    
    start_time = time.time()
    
    return data.filter(pl.col('overall') > 60)

def sort_by_short_name_file1(data: pl.LazyFrame) -> pl.LazyFrame:
   # Comentario explicativo de la funcion
   
    return data.sort("short_name")


def sort_by_potential_file2(data: pl.LazyFrame) -> pl.LazyFrame:
   # Comentario explicativo de la funcion
   
    return data.sort("potential")

def creation_of_cross_file2(data: pl.LazyFrame) -> pl.LazyFrame:
   # Comentario explicativo de la funcion
   
    df_return = data.with_columns(
        [
            (pl.col('overall')*pl.col('potential')).alias('cross'),
            (pl.col('potential')/pl.col('overall')).alias('cross2'),
            (pl.col('overall') + pl.col('potential')).alias('cross3'),
        ]
    )
    return df_return


def groupby_short_name_file2(data: pl.LazyFrame) -> pl.LazyFrame:
# Comentario explicativo de la funcion


    df_return = (
    data
    .groupby(["short_name"])
    .agg(
        [
            pl.col("cross").mean(),
            pl.col('cross2').mean(),
            pl.col('cross3').mean()
        ]
    )
)
    return df_return


def inner_join_over_short_name(data_left: pl.LazyFrame,
                               data_right: pl.LazyFrame) -> pl.LazyFrame:
    
    # Comentario explicativo de la funcion
    return data_left.join(data_right, on="short_name", how="inner")

def filter_potential_over_66_file1(data: pl.LazyFrame) -> pl.LazyFrame:
    # Comentario explicativo de la funcion
    return data.filter(pl.col('potential') > 66)

def group_by_shor_tname_joined_data(data: pl.LazyFrame) -> pl.LazyFrame:
   # Comentario explicativo de la funcion
   
    data = (data
            .groupby(["short_name"])
            .agg(
                [
                    pl.col("overall").mean(),
                    pl.col('potential').mean(),
                    pl.col('cross').mean(),
                    pl.col('cross2').mean(),
                    pl.col('cross3').mean()
                ]
            )
        )
    
    return data

def sort_by_short_name_joined_data(data: pl.LazyFrame) -> pl.LazyFrame:
# Comentario explicativo de la funcion


    
    data_to_return = data.sort('short_name').collect()
    print(time.time()-start_time)
    print(data_to_return.head(4))
    return data_to_return




###################################### PANDAS ################################


def conversion_to_polars_file1() -> pd.DataFrame:
# Comentario explicativo de la funcion


    return pd.read_csv('data/01_raw/male_players_1million_2_simplified.csv')


def conversion_to_polars_file2() -> pd.DataFrame:
   # Comentario explicativo de la funcion
   
    return pd.read_csv('data/01_raw/male_players_1million_1_simplified.csv')



def filter_overall_over_60_file1(data: pd.DataFrame) -> pd.DataFrame:
# Comentario explicativo de la funcion


    global start_time
    
    start_time = time.time()
    
    return data[data['overall'] > 60]

def sort_by_short_name_file1(data: pd.DataFrame) -> pd.DataFrame:
   # Comentario explicativo de la funcion
   
    return data.sort_values('short_name')


def sort_by_potential_file2(data: pd.DataFrame) -> pd.DataFrame:
   # Comentario explicativo de la funcion
   
    return data.sort_values("potential")

def creation_of_cross_file2(data: pd.DataFrame) -> pd.DataFrame:
   # Comentario explicativo de la funcion
   
    df_return = data
    df_return['cross'] = df_return['potential'] * df_return['overall']
    df_return['cross2'] = df_return['potential'] / df_return['overall']
    df_return['cross3'] = df_return['potential'] + df_return['overall']

    return df_return


def groupby_short_name_file2(data: pd.DataFrame) -> pd.DataFrame:
    # Comentario explicativo de la funcion


    
    return data.groupby("short_name", as_index=False).agg({"cross": "mean",
                                                           "cross2": "mean",
                                                           "cross3": "mean",})


def inner_join_over_short_name(data_left: pd.DataFrame,
                               data_right: pd.DataFrame) -> pd.DataFrame:
    # Comentario explicativo de la funcion
    
    return data_left.merge(data_right, on="short_name", how="inner", suffixes=(None, None))

def filter_potential_over_66_file1(data: pd.DataFrame) -> pd.DataFrame:
    # Comentario explicativo de la funcion


    return data[data['potential'] > 66]

def group_by_shor_tname_joined_data(data: pd.DataFrame) -> pd.DataFrame:
   # Comentario explicativo de la funcion
   
    data = data.groupby("short_name", as_index=False).agg({"overall": "mean",
                                                           "potential": "mean",
                                                           "cross": "mean",
                                                           "cross2": "mean",
                                                           "cross3": "mean",})
    
    return data

def sort_by_short_name_joined_data(data: pd.DataFrame) -> pd.DataFrame:
# Comentario explicativo de la funcion


    
    data_to_return = data.sort_values('short_name')
    print(time.time()-start_time)
    print(data_to_return.head(4))
    return data_to_return
    
    
'''

