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

    # Cargar datos desde el catálogo
    customer_curated_data = glueContext.create_dynamic_frame.from_catalog(
        database="project",
        table_name="customer_curated",
        transformation_ctx="customer_curated_data",
    )

    # Cargar datos desde S3
    step_trainer_options = {
        "format_options": {"multiline": False},
        "connection_type": "s3",
        "format": "json",
        "connection_options": {
            "paths": ["s3://glue-spark-bucket-ami/project/step_trainer/landing/"],
            "recurse": True,
        },
        "transformation_ctx": "step_trainer_options",
    }
    step_trainer_data = glueContext.create_dynamic_frame.from_options(**step_trainer_options)

    # Realizar una unión de datos
    joined_data = Join.apply(
        frame1=step_trainer_data,
        frame2=customer_curated_data,
        keys1=["serialNumber"],
        keys2=["serialnumber"],
        transformation_ctx="joined_data",
    )

    # Eliminar campos no deseados
    drop_fields = DropFields.apply(
        frame=joined_data,
        paths=[
            "customername",
            "email",
            "phone",
            "birthday",
            "serialnumber",
            "registrationdate",
            "lastupdatedate",
            "sharewithresearchasofdate",
            "sharewithfriendsasofdate",
            "sharewithpublicasofdate",
        ],
        transformation_ctx="drop_fields",
    )

    # Escribir datos en el catálogo de AWS Glue
    glueContext.write_dynamic_frame.from_catalog(
        frame=drop_fields,
        database="project",
        table_name="step_trainer_trusted",
        transformation_ctx="glue_catalog",
    )

    job.commit()

if __name__ == "__main__":
    main()
