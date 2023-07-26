from pyspark.sql.functions import length , max 

df = spark.table("leadallocation_conformed_uat.lead_submissions_nonprod_export")
cols = df.schema.names 

# test =  df.agg(max(length('loannumber'))).alias("loannumber_len").first()
# row = test[0]
# print ( row )
# print(test)
# print(row)

for col in cols:
    test = df.agg(max(length(col))).first()
    row = test[0]
    # print (row)
    # print(test)
    print('"' + str(col) + '"  ' + 'varchar(' + str(row) + '),')
