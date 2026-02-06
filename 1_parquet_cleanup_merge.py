import polars as pl
import glob
import os
import pandas as pd

# step 1: define paths
file_pattern = '/Users/annatymoshenko/Desktop/Docs/Data Analysis//Backlink_check/temy_parquet_output/*.parquet'
excel_path = '/Users/annatymoshenko/Desktop/Docs/Data Analysis/Backlink_check/input_links_sistrix.xlsx' 
output_path = '/Users/annatymoshenko/Desktop/Docs/Data Analysis/Backlink_check/parquet_sistrix_cleaned.xlsx' 
output_path_csv='/Users/annatymoshenko/Desktop/Docs/Data Analysis/Backlink_check/output_parquet_sistrix_merged.csv' 

# step 2: getting the excel data to work with, not loading it to the RAM
excel_df = pl.read_excel(excel_path).lazy()


# step 3: confirm what column actually exist using glob and then compare if columns that I am interested in are in the parquets
actual_columns = pl.read_parquet_schema(glob.glob(file_pattern)[0]).keys()

target_columns = [
    'from_url_id', 'source_pair_id', 'from_url', 'from_domain',
    'status_code', 'robots_content', 'language', 'relevant_follow_links',
    'is_redirected', 'categories', 'topics', 'keywords'
]

# Only select the target columns that exist in the actual files and actual columns to avoid errors
existing_targets = [col for col in target_columns if col in actual_columns]

# step 4: bulding lazy query - saving RAM
lazy_df = pl.scan_parquet(file_pattern) # just scanning files wihout loading any data into RAM

query = (
    lazy_df
    .select(existing_targets)
    .filter(
        pl.col("relevant_follow_links").is_not_null() & # only selecting rows where this column is filled with values
        (pl.col("relevant_follow_links").list.len() > 0)
    )
    .explode("relevant_follow_links") # ensuring each link in the cell gets it's own row (sometimes there are multiple links packed together in a cell)
    .join(#attempting to merge two files based on two columns in each - doesn't work for some reason
        excel_df, 
        left_on=["from_url", "relevant_follow_links"], # columns in the Parquet data
        right_on=["from_url", "to_url"], # columns in the excel data
        how="inner" #checks for the data to exist in both files and leave only those rows
    )
    # remove any duplicates created by the join, if there is a raw that has same values from_url and in "relevant_follow_links" it will be deleted
    .unique(subset=["from_url", "relevant_follow_links"], keep="first")
    # get the document ID (number) from the URL
    .with_columns( # commands to add a new column
        pl.col("relevant_follow_links") # commands to extract the data for the new column from the specified column
        .str.extract(r"document/(?:read|view)/(\d+)", 1) 
            # matches the literal text "document/" in the URL
            # looks for either the word "read" or "view
        .alias("extracted_id") # gives a name to the new column
    )
# step 5:  keep only rows where an ID was successfully found, everything else doesn't interest me now
    .filter(pl.col("extracted_id").is_not_null())
# step 6: final column selection and order
    .select([
        "from_url_id", "source_pair_id", "from_url", "from_domain",
        "status_code", "robots_content", "language", "relevant_follow_links",
        "extracted_id", "is_redirected", "categories", "topics", "keywords"
    ])
)

# step 7: ensure the folder for the output file is there
output_dir = os.path.dirname(output_path_csv) #"chops off" the filename at the end to get only the folder path
if not os.path.exists(output_dir): # is checking if a folder with this name already exist on the hard drive
    os.makedirs(output_dir) # if the folder wasn't found, this command creates it.

# step 8 : save all the data in the final file (victory!)
print("Merging data and streaming to CSV...")

try:
    query.sink_csv(output_path_csv, separator=";") # shouldn't use write_csv here because working with a lot of data, sink_csv makes the script process all the data in pieces to not eat up my RAM
    print(f"Success! Merged file saved to: {output_path_csv}") # jej!!
except Exception as e:
    print(f"Failed to write file: {e}") # oh no :(
    
