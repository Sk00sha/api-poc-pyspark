from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType
from .reader import build_schema, fetch_partition_from_ids

class RestAPIPartitionedSource(DataSource):
    @classmethod
    def name(cls):
        return "rest_api_partitioned_source"

    def schema(self, options):
        return build_schema(options)

    def reader(self, schema, options):
        return RestAPIPartitionedReader(schema, options)


class RestAPIPartitionedReader(DataSourceReader):
    def __init__(self, schema, options):
        self.schema_obj = schema
        self.options = options

    def partitions(self):
        """
        Convert a DataFrame column into Spark InputPartitions without collecting.
        Each partition will receive a subset of IDs from the DataFrame.
        """
        df_ids = self.options["id_column_df"].select(self.options["id_column_name"])
        partition_size = int(self.options.get("partition_size", 1000))

        # Repartition DataFrame to control number of executor partitions
        num_partitions = max(1, df_ids.count() // partition_size)
        df_ids_repart = df_ids.repartition(num_partitions)

        # Convert each Spark partition to InputPartition
        partitions = []
        for i, rdd_partition in enumerate(df_ids_repart.rdd.glom().collect()):
            # rdd_partition is a list of Rows in that Spark partition
            ids = [row[0] for row in rdd_partition]
            partitions.append(RestAPIInputPartition(ids))
        return partitions

    def read(self, partition: InputPartition):
        return fetch_partition_from_ids(partition.ids, self.schema_obj, self.options)


class RestAPIInputPartition(InputPartition):
    def __init__(self, ids):
        self.ids = ids
