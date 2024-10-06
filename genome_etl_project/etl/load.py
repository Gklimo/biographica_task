def load_to_delta_lake(dataframe, delta_path):
    dataframe.write.format("delta").mode("overwrite").save(delta_path)
    print(f"Raw data loaded to Delta table at {delta_path}")
