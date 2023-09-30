from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task()
def fetch(dataset_url:str) ->pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df

@task(log_prints=True)
def clean(df=pd.DataFrame) ->pd.DataFrame:
    """Fix some dtypes issues"""
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    return df

@task()
def write_local(df:pd.DataFrame, color:str, dataset_file:str) -> Path:
    """Write dataframe out locally as an parquet file"""
    path = Path(f"{dataset_file}.parquet")
    path_full = Path(f"data/{color}/{path}")
    df.to_parquet(path, compression='gzip')
    print(f"len of df {len(df)}")
    return path, path_full

@task()
def write_gcs(to_path:Path, from_path:Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block= GcsBucket.load('zoom-gcs')
    gcs_block.upload_from_path(
        from_path=f"{from_path}",
        to_path=f"{to_path}"
    )
    return
@flow()
def etl_web_to_gcs() -> None:
    """The main ETL function"""
    color = "green"
    year = 2020
    months = [11]
    
    for month in months:
        dataset_file = f"{color}_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"
        df = fetch(dataset_url)
        df_clean = clean(df)
        from_path,to_path = write_local(df_clean,color, dataset_file)
        write_gcs(to_path,from_path)


if __name__ == '__main__':
    etl_web_to_gcs()

