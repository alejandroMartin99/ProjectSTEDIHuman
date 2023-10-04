import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    # Cargar datos desde S3
    s3_customer_options = {
        "format_options": {"multiline": False},
        "connection_type": "s3",
        "format": "json",
        "connection_options": {
            "paths": ["s3://glue-spark-bucket-ami//project/customers/landing/"],
            "recurse": True,
        },
        "transformation_ctx": "s3_customer_options",
    }
    s3_customer_data = glueContext.create_dynamic_frame.from_options(**s3_customer_options)

    # Aplicar filtro
    filtered_data = Filter.apply(
        frame=s3_customer_data,
        f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
        transformation_ctx="filtered_data",
    )

    # Escribir datos en el catálogo de AWS Glue
    glueContext.write_dynamic_frame.from_catalog(
        frame=filtered_data,
        database="project",
        table_name="customer_trusted",
        transformation_ctx="glue_catalog",
    )

    job.commit()

if __name__ == "__main__":
    main()
