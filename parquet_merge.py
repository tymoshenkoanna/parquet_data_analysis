import pandas as pd
#why this additional code? for some reason the merging didn't quite work in the first one and there was no time to troubleshoot, living it for later
# step 1: defining the file paths
parquet_input = '/Users/annatymoshenko/Desktop/Docs/Data Analysis/Backlink_check/output_parquet_sistrix_merged.xlsx'
sistrix_input = '/Users/annatymoshenko/Desktop/Docs/Data Analysis/Backlink_check/input_links_sistrix.xlsx'
output_path = '/Users/annatymoshenko/Desktop/Docs/Data Analysis/Backlink_check/merged_results.xlsx'

print("Loading files...")

# step 2: loading the datasets
df_parquet = pd.read_excel(parquet_input)
df_sistrix = pd.read_excel(sistrix_input)

# step 3: ensure "from_url" in sistrix is unique (extract once per match)
# This prevents the merged file from bloating and ensures a clean join
df_sistrix_unique = df_sistrix.drop_duplicates(subset=['from_url'])

print(f"Merging data on 'from_url'...")

# step 4: performing the MEEEERGE :)
# 'left' merge keeps all rows from my primary file (parquet)
merged_df = pd.merge(df_parquet, df_sistrix_unique, on='from_url', how='left')

print(f"Exporting to Excel (hyperlink limit fix applied)...")

# step 5: exporting to Excel with the Fix
# using the 'xlsxwriter' engine to disable the automatic conversion of strings to clickable links.
# This bypasses the 65,530 hyperlink limit and prevents the IOPub data rate error = "safety net" for handling large datasets in Excel.
with pd.ExcelWriter(output_path, engine='xlsxwriter', engine_kwargs={'options': {'strings_to_urls': False}}) as writer:# telling the system to trat URLs not as links but as plain text, due to the MS limits for the amount of links in the file
    merged_df.to_excel(writer, index=False) # actually merging the data into the file

print("-" * 30)
print(f"Yeeeey!")
print(f"Final row count: {len(merged_df)}")
print(f"File saved to: {output_path}")
print("-" * 30)
