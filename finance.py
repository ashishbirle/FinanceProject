from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name
import glob

spark = SparkSession.builder.appName("Finance").getOrCreate()

from pyspark.sql import functions as F

# Validation on Model Collateral File
print("Performing validations on model collateral file.")
model_collateral_raw = spark.read.csv(r"C:\Users\Ashish Birle\OneDrive\Desktop\Data Engineering\FRS Project Files\data\model_collateral.csv", header=True, inferSchema=True)
model_collateral_raw.show(5)

print("row_count",model_collateral_raw.count() )  #----> check row_count

print("col_count",len(model_collateral_raw.columns))   #-----> check col_count

def validate_dataframe_collateral(model_collateral_raw,n_cols=None,check_duplicates=False,
                       check_nulls=False):
    
    if n_cols and len(model_collateral_raw.columns) != n_cols:
        return False, f"Expected {n_cols} columns but got {len(model_collateral_raw.columns)}"
    
    if check_duplicates and model_collateral_raw.count() != model_collateral_raw.dropDuplicates().count():
        return False, "Duplicates found"
    
    if check_nulls and model_collateral_raw.filter(model_collateral_raw.isNull()).count() > 0:
        return False, "Null values found"
    
    return True, "DataFrame passed validation"

is_valid, message = validate_dataframe_collateral(model_collateral_raw, n_cols=78, check_duplicates=True)
print(is_valid, message)

if is_valid == True:
    model_collateral = model_collateral_raw
else:
    print("The dataframe model_collateral_raw doesn't pass validation.")

print()
print("-------------- -------------- ----------------")
print()

# Validation on Model Config File
print("Performing validations on model config file.")
model_config_raw = spark.read.csv(r"C:\Users\Ashish Birle\OneDrive\Desktop\Data Engineering\FRS Project Files\data\model_config.csv", header=True, inferSchema=True)
model_config_raw.show(5)

print("row_count",model_config_raw.count() )  #----> check row_count

print("col_count",len(model_config_raw.columns))   #-----> check col_count

def validate_dataframe_config(model_config_raw,n_cols=None,check_duplicates=False,
                       check_nulls=False):
    
    if n_cols and len(model_config_raw.columns) != n_cols:
        return False, f"Expected {n_cols} columns but got {len(model_config_raw.columns)}"
    
    if check_duplicates and model_config_raw.count() != model_config_raw.dropDuplicates().count():
        return False, "Duplicates found"
    
    if check_nulls and model_config_raw.filter(model_config_raw.isNull()).count() > 0:
        return False, "Null values found"
    
    return True, "DataFrame passed validation"

is_valid, message = validate_dataframe_config(model_config_raw, n_cols=4, check_duplicates=True)
print(is_valid, message)

if is_valid == True:
    model_config = model_config_raw
else:
    print("The dataframe model_config_raw doesn't pass validation.")

print()
print("-------------- For model auth rep folder ----------------")
print()

# getting files from auth_Rep folder
folder = r"C:\Users\Ashish Birle\OneDrive\Desktop\Data Engineering\FRS Project Files\data\model_auth_Rep"
file_list = glob.glob(folder + r"\*.csv")

#model_auth_rep_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(file_list).withColumn("source_file", input_file_name())
model_auth_rep_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(file_list)

model_auth_rep_raw.show(5)
print("Total number of records are:", model_auth_rep_raw.count())

print("row_count",model_auth_rep_raw.count() )  #----> check row_count

print("col_count",len(model_auth_rep_raw.columns))   #-----> check col_count

def validate_dataframe(model_auth_rep_raw,n_cols=None,check_duplicates=False,
                       check_nulls=False):
    
    if n_cols and len(model_auth_rep_raw.columns) != n_cols:
        return False, f"Expected {n_cols} columns but got {len(model_auth_rep_raw.columns)}"
    
    if check_duplicates and model_auth_rep_raw.count() != model_auth_rep_raw.dropDuplicates().count():
        return False, "Duplicates found"
    
    if check_nulls and model_auth_rep_raw.filter(model_auth_rep_raw.isNull()).count() > 0:
        return False, "Null values found"
    
    return True, "DataFrame passed validation"

is_valid, message = validate_dataframe(model_auth_rep_raw, n_cols=14, check_duplicates=True)
print(is_valid, message)

if is_valid == True:
    model_auth_rep = model_auth_rep_raw
else:
    print("The dataframe needs to pass validation for further operation")

# Stage 1 ECL
model_auth_rep = model_auth_rep.withColumn("stage1ecl", F.col("EAD") * F.col("PD12") * F.col("LGD"))

# Stage 2 ECL
model_auth_rep = model_auth_rep.withColumn("stage2ecl", F.col("EAD") * F.col("PDLT") * F.col("LGD"))


# Stage 3 ECL
model_auth_rep = model_auth_rep.withColumn("stage3ecl", F.col("EAD") * F.col("LGD"))
ecl_dataframe = model_auth_rep.select("EAD", "PD12", "LGD", "PDLT", "stage1ecl", "stage2ecl", "stage3ecl")
print("Expecting Credit Loss (ECL) Report")
ecl_dataframe.show(5)

#ecl_dataframe.coalesce(1).write.option("header", "true").mode("overwrite").parquet("output/ecl_dataframe_csv")
#ecl_dataframe.write.mode("overwrite").option("header", True).csv("output/ecl_dataframe1_csv")

## EAD Variation Reports
# change_EAD = EAD - Previous EAD

model_auth_rep = model_auth_rep.withColumn("change_EAD", F.col("EAD") - F.col("Previous EAD"))

#  percentage_change_EAD = ((EAD - Previous EAD) / Previous EAD) * 100
model_auth_rep = model_auth_rep.withColumn("percentage_change_EAD", ((F.col("EAD") - F.col("Previous EAD")) / F.col("Previous EAD")) * 100)

EAD_model_auth_rep = model_auth_rep.select("EAD", "Previous EAD", "change_EAD", "percentage_change_EAD")

print("Exposure at Default (EAD) Report")
EAD_model_auth_rep.show(5)

# writing the dataframe
#EAD_model_auth_rep.coalesce(1).write.option("header", "true").mode("overwrite").csv("output/EAD_model_auth_rep_csv")
#EAD_model_auth_rep.write.mode("overwrite").option("header", True).csv("output/EAD_model_auth_rep1_csv")

# LGD variation reports 
# change_LGD = LGD - Previous LGD

model_auth_rep = model_auth_rep.withColumn("change_LGD", F.col("LGD") - F.col("Previous LGD"))

# percentage_change_LGD = ((LGD - Previous LGD) / Previous LGD) * 100
model_auth_rep = model_auth_rep.withColumn("percentage_change_LGD", ((F.col("LGD") - F.col("Previous LGD"))/F.col("Previous LGD")) * 100)

LGD_model_auth_rep = model_auth_rep.select("LGD", "Previous LGD", "change_LGD", "percentage_change_LGD")

print("Loss Given Default (LGD) Report")
LGD_model_auth_rep.show(5)

# writing the dataframe
#LGD_model_auth_rep.coalesce(1).write.option("header", "true").mode("overwrite").csv("output/LGD_model_auth_rep_csv")
#LGD_model_auth_rep.write.mode("overwrite").option("header", True).csv("output/LGD_model_auth_rep1_csv")

print("All project run successfully")