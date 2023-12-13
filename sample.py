import dlt
from chess import source
if __name__ == "__main__" :
    pipeline = dlt.pipeline(pipeline_name="chess_pipeline", destination='duckdb', dataset_name="games_data")
    # get data for a few famous players
    data = source(['magnuscarlsen','vincentkeymer', 'dommarajugukesh', 'rpragchess'], start_month="2022/11", end_month="2022/12")
    load_info = pipeline.run(data)