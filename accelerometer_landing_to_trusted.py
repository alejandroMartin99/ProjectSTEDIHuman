import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

def main():
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    # Cargar datos del catálogo
    accelerometer_catalog = glueContext.create_dynamic_frame.from_catalog(
        database="project",
        table_name="accelerometer_landing",
        transformation_ctx="accelerometer_catalog",
    )

    # Cargar datos desde S3
    customer_options = {
        "format_options": {"multiline": False},
        "connection_type": "s3",
        "format": "json",
        "connection_options": {
            "paths": ["s3://glue-spark-bucket-ami/project/customers/trusted/"],
            "recurse": True,
        },
        "transformation_ctx": "customer_options",
    }
    customer_trusted_zone = glueContext.create_dynamic_frame.from_catalog(
        **customer_options
    )

    # Realizar una unión de datos
    joined_data = Join.apply(
        frame1=customer_trusted_zone,
        frame2=accelerometer_catalog,
        keys1=["email"],
        keys2=["user"],
        transformation_ctx="joined_data",
    )

    # Eliminar campos no deseados
    drop_fields = DropFields.apply(
        frame=joined_data,
        paths=[
            "serialNumber",
            "shareWithPublicAsOfDate",
            "birthDay",
            "registrationDate",
            "shareWithResearchAsOfDate",
            "customerName",
            "email",
            "lastUpdateDate",
            "phone",
            "shareWithFriendsAsOfDate",
            "timestamp",
        ],
        transformation_ctx="drop_fields",
    )

    # Escribir datos en el catálogo de AWS Glue
    glueContext.write_dynamic_frame.from_catalog(
        frame=drop_fields,
        database="project",
        table_name="accelerometer_trusted",
        transformation_ctx="glue_catalog",
    )

    job.commit()

if __name__ == "__main__":
    main()
