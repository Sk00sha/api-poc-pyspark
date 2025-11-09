from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType, StructField, StringType
from .reader import build_schema, fetch_partition_from_ids

class RestAPIPartitionedSource(DataSource):
    @classmethod
    def name(cls):
        return "rest_api_partitioned_source"

    def schema(self, options):
        # sample_param used to fetch schema
        return build_schema(options)

    def reader(self, schema, options):
        # options["id_column_df"] = DataFrame
        # options["id_column_name"] = column name in that DataFrame
        return RestAPIPartitionedReader(schema, options)


class RestAPIPartitionedReader(DataSourceReader):
    def __init__(self, schema, options):
        self.schema_obj = schema
        self.options = options

    def read(self, partition: InputPartition):
        # fetch partition-specific IDs
        return fetch_partition_from_ids(partition.ids, self.schema_obj, self.options)

    def partitions(self):
        # get IDs from input DataFrame column
        df_ids = self.options["id_column_df"].select(self.options["id_column_name"])
        id_list = [row[0] for row in df_ids.collect()]  # collect on driver, may use limit for very large datasets

        # split into partition chunks
        partition_size = int(self.options.get("partition_size", 1000))
        partitions = []
        for i in range(0, len(id_list), partition_size):
            partitions.append(RestAPIInputPartition(id_list[i:i+partition_size]))
        return partitions


class RestAPIInputPartition(InputPartition):
    def __init__(self, ids):
        self.ids = ids
