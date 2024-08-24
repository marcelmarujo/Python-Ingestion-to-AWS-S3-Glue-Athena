from csvToParquet import process_csv

def main():
    csv_file = '../data.moviesAndTv.csv'
    output_dir = '../output_files'
    bucket_name = 'sql-athena-parquet'
    s3_key = 'parquet_files/movies_and_tv.parquet'
    
    process_csv(csv_file, output_dir, bucket_name, s3_key)
    
    if __name__ == "__main__":
        main()
        
        #.