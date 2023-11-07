from pyspark.sql import SparkSession
import pyspark.sql.functions as f

# Initialize a Spark session
spark = SparkSession.builder.appName("IdentifyDuplicates").getOrCreate()

# Create another dataframe with different data but some duplicate columns
data3 = [(None, 200, 300, 400), (400, 500, 600, None), (700, 800, 900, 100)]
columns3 = ["A", "B", "a", "a"]
df = spark.createDataFrame(data3, columns3)

class SparkDfCleaner():
    def __init__(self, spark_df):
        self.df = spark_df
        self.metadata_dict = {}
        self.column_names = [item.lower() for item in self.df.columns]

    def identify_duplicate_col(self):
        duplicate_columns = set()
        seen = set()
        for col in self.column_names:
            col_name = col.lower()
            if col_name in seen:
                duplicate_columns.add(col_name)
            seen.add(col_name)

        # Print the duplicate column names
        if duplicate_columns:
            print(">> Duplicate column names found.")
            duplicate_col_dict = {}
            for idx, value in enumerate(self.column_names):
                if value.lower() in duplicate_columns:
                    try:
                        duplicate_col_dict[value.lower()].append(idx)
                    except:
                        duplicate_col_dict[value.lower()] = []
                        duplicate_col_dict[value.lower()].append(idx)
                else:
                    pass
            self.metadata_dict = duplicate_col_dict
            print(self.metadata_dict)
            return True
        else:
            print(">> No duplicate column names found.")
            return False

    def merge_duplicate_col(self):
      try: 
        print(">> Merging duplicate columns")
        for key, value in self.metadata_dict.items():
            for i in value:
                self.column_names[i] = key + '_duplicate_' + str(i)
        self.df = self.df.toDF(*self.column_names)
        for key, value in self.metadata_dict.items():
            duplicate_columns = []
            for i in value:
                col_name = key + "_duplicate_" + str(i)
                duplicate_columns.append(col_name)
            print(f">> Merging column : {duplicate_columns}")
            duplicate_col = [f.col(i) for i in duplicate_columns]
            self.df = self.df.select('*', f.concat_ws(' ', *duplicate_col).alias(key))
            self.df = self.df.drop(*duplicate_columns)
        print(">> Column Merged")
      except Exception as e:
        print(e)

    def return_df(self):
        return self.df
    
    def main(self):
        flag = self.identify_duplicate_col()
        if flag:
            self.merge_duplicate_col()
            return self.df
        else:
            print(">> No changes made to Spark_df")
            return self.df
            
        
        

# Show the original dataframe
df.show()

# Create an instance of the SparkDfCleaner class
handler = SparkDfCleaner(df)
spark_df = handler.main()
spark_df.show()
