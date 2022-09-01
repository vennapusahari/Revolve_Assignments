import argparse
from pyspark.sql.functions import *
import logging



def get_params() -> dict:
    parser = argparse.ArgumentParser(description='DataTest')
    parser.add_argument('--customers_location', required=False, default="./input_data/starter/customers.csv")
    parser.add_argument('--products_location', required=False, default="./input_data/starter/products.csv")
    parser.add_argument('--transactions_location', required=False, default="./input_data/starter/transactions/")
    parser.add_argument('--output_location', required=False, default="./output_data/outputs/")
    return vars(parser.parse_args())


## Reading data from csv
def read_csv(file_path: str):
    
    try:
        
        csv_df = spark.read \
                .format("csv") \
                .option("header",True) \
                .option("inferSchema",True)\
                .load(file_path)
                
    except Exception as e:
        
        logging.exception(e)
        raise e
    
    logging.info(f"successfully read the csv file at {file_path}")
    return csv_df


 
## writing data to csv file 
def write_csv(df, file_path: str):
    
    try:
        
        df.write \
          .format("csv") \
          .mode("overwrite") \
          .save(file_path)
          
    except Exception as e:
        
        logging.exception(e)
        raise e
    
    logging.info(f"successfully written the csv file at {file_path}")


## reading json data
def read_json(file_path: str):       
    
    try:
        json_df = spark.read \
                .option("header",True) \
                .json(file_path)
                
    except Exception as e:
        
        logging.exception(e)
        raise e
    
    logging.info(f"successfully read the json file at {file_path}")
        
    return json_df


    
## Exploding the nested jsons and json arrays  
def explode_json_date(df):
    
    explde_df = df.withColumn("baskets",explode("basket")) \
                  .select("customer_id","date_of_purchase","baskets.*")
    
    return explde_df


## joining two data frames    
def join_dfs(df1,df2, join_condition : str, join_type="inner"):
    
    result_df = df1.join(df2, on=join_condition, how=join_type)
    
    return result_df
 


## finding the patterns 
def get_patterns(params : dict):
    
    ## getting customer, product, transactions data frames
    customer_df = read_csv( params["customers_location"] )
    products_df = read_csv( params["products_location"] )
    transactions_df = explode_json_date( read_json( params["transactions_location"] ) )
    
    ## joining data frames
    cus_tans_df = join_dfs( customer_df, transactions_df , join_condition = "customer_id" )
    final_df = join_dfs( cus_tans_df, products_df , join_condition = "product_id")
    
    ## selecting necessary columns and grouping to get the product count
    output_df = final_df.select("customer_id","loyalty_score","product_id","product_category") \
                        .groupBy("customer_id","loyalty_score","product_id","product_category") \
                        .count()
                        
    return output_df
                        
    

def main():
    
    logging.info(f"Started exectuing solution_start.py .....")
    
    try:
    
        ## getting params
        params = get_params()        
        
        output_df = get_patterns(params)
        
        write_csv( output_df, params["output_location"] )
        
    except Exception as e:
        
        logging.exception(e)
        raise e

    
    logging.info(f"Completed exectuing solution_start.py .....")
    

if __name__ == "__main__":
    main()
