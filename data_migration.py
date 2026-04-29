import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
# Script generated for node Total Value
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.functions import col

    # Get the input DynamicFrame
    dyf = dfc.select(list(dfc.keys())[0])

    # Convert to DataFrame
    df = dyf.toDF()

    # Multiply two columns
    df = df.withColumn(
        "total_value",
        col("Price") * col("Quantity")
    )

    # Convert back to DynamicFrame
    result_dyf = DynamicFrame.fromDF(df, glueContext, "result_dyf")

    return DynamicFrameCollection({"result": result_dyf}, glueContext)
def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1775192884026 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://bucket-sm-1-1/visual/"], "recurse": True}, transformation_ctx="AmazonS3_node1775192884026")

# Script generated for node Drop Duplicates
DropDuplicates_node1775193797088 =  DynamicFrame.fromDF(AmazonS3_node1775192884026.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1775193797088")

# Script generated for node Change Schema
ChangeSchema_node1775193930272 = ApplyMapping.apply(frame=DropDuplicates_node1775193797088, mappings=[("OrderID", "string", "OrderID", "string"), ("Customer", "string", "Customer", "string"), ("Item", "string", "Item", "string"), ("Quantity", "string", "Quantity", "bigint"), ("Price", "string", "Price", "double"), ("OrderDate", "string", "OrderDate", "string")], transformation_ctx="ChangeSchema_node1775193930272")

# Script generated for node Total Value
TotalValue_node1775193865095 = MyTransform(glueContext, DynamicFrameCollection({"ChangeSchema_node1775193930272": ChangeSchema_node1775193930272}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1775195123939 = SelectFromCollection.apply(dfc=TotalValue_node1775193865095, key=list(TotalValue_node1775193865095.keys())[0], transformation_ctx="SelectFromCollection_node1775195123939")

# Script generated for node SQL Query
SqlQuery24 = '''
select * from myDataSource
WHERE total_value>1000
'''
SQLQuery_node1775194846912 = sparkSqlQuery(glueContext, query = SqlQuery24, mapping = {"myDataSource":SelectFromCollection_node1775195123939}, transformation_ctx = "SQLQuery_node1775194846912")

# Script generated for node Aggregate
Aggregate_node1775198193237 = sparkAggregate(glueContext, parentFrame = SQLQuery_node1775194846912, groups = ["Item"], aggs = [["total_value", "sum"]], transformation_ctx = "Aggregate_node1775198193237")

# Script generated for node Evaluate Data Quality
EvaluateDataQuality_node1775199294658_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [
    ColumnCount=7,
    ColumnValues"total_value">1000
    ]
"""

EvaluateDataQuality_node1775199294658 = EvaluateDataQuality().process_rows(frame=SQLQuery_node1775194846912, ruleset=EvaluateDataQuality_node1775199294658_ruleset, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1775199294658", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"observations.scope":"ALL","performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node Rename Field
RenameField_node1775198767396 = RenameField.apply(frame=Aggregate_node1775198193237, old_name="`sum(total_value)`", new_name="sumtotal_value", transformation_ctx="RenameField_node1775198767396")

# Script generated for node Select From Collection
SelectFromCollection_node1775199657897 = SelectFromCollection.apply(dfc=EvaluateDataQuality_node1775199294658, key="Select From Collection", transformation_ctx="SelectFromCollection_node1775199657897")

# Script generated for node Aggregation S3
EvaluateDataQuality().process_rows(frame=RenameField_node1775198767396, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1775192881279", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
additional_options = {"path": "s3://bucket-sm-1-1/S3sinkAgg/", "write.parquet.compression-codec": "snappy"}
AggregationS3_node1775195388478_df = RenameField_node1775198767396.toDF()
AggregationS3_node1775195388478_df.write.format("delta").options(**additional_options).partitionBy("Item").mode("append").save()

# Script generated for node sink S3
sinkS3_node1775195333715 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1775199657897, connection_type="s3", format="glueparquet", connection_options={"path": "s3://bucket-sm-1-1/S3sink/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="sinkS3_node1775195333715")

job.commit()