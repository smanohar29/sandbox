# Unzip files

import sys, os
import tarfile
import zipfile

sys.path.insert(0, os.getcwd())

tar = tarfile.open("configprops.tar.gz", "r:gz")
tar.extractall(os.getcwd())

with zipfile.ZipFile("apps.zip","r") as zip_ref:
    zip_ref.extractall(os.getcwd())


#

config = {
                "stagingBucketName":"ql-dl-offline-featurestore-uat-814128445328-us-east-2",
                "stagingFileBasePrefix":"offline_tables/RocketScience/LeadAllocation/LeadQuality/lq_conversion_current_uat",
                "gatewayBucketName":"prod-206823-leadquality",
                "gatewayFilePrefix":"",
                "timezone": "US/Eastern",
                "dateformat": "",
                "filesConfig":
                    {
                    "scores": {
                        "sourceFolder": "",
                        "destinationFileName": "maestro_leadquality_scores",
                        "destinationFileExt": ".csv"
                    }
                }
            }

stagingBucketName =  config['stagingBucketName']
stagingFileBasePrefix = config['stagingFileBasePrefix']

s3path = "s3a://{0}/{1}/".format(stagingBucketName, stagingFileBasePrefix)

print(s3path)


