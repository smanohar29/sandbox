snsSubject = "Digital Data Partition Mismatch Alert"
errorDescription = "The error is for missing partitions for the tables/S3 paths mentioned below"
targetTables = "rktdp_adobe_omniture_raw_access.adobe_rawdata_hourly"
frequency = "hourly"
partition1 = "2023=02-17"
# partition2 = None
partition2 = "11"


error_message = """
        ------------------------------------------------------------------------------------
        Job Status :  {snsSubject}
        ------------------------------------------------------------------------------------ 
        Message Info : {errorDescription}
        Error Tables: {targetTables}
        Scheuled Job Cadence: {frequency}
        Impacted Partition(s): {partition1}{partition2}
        
        ------------------------------------------------------------------------------------ 
        """

if partition2 is None:
    partition2 = ''
else:
    partition1 = partition1 + ' / '

error_message = error_message.format(snsSubject=snsSubject, errorDescription=errorDescription, targetTables=targetTables,
               partition1=partition1, partition2=partition2, frequency=frequency)

print(error_message)