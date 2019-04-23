
from axs.axsframe import AxsFrame
from axs import Constants
from pyspark.sql import functions as F


class AxsCatalog:
    """
    Implements high-level operations on AXS tables, such as loading, saving, renaming and dropping. AXS tables are
    Spark Parquet tables, but are bucketed and sorted in a special way. `AxsCatalog` relies on Spark Catalog for
    loading table data and it needs an active `SparkSession` object (with Hive support enabled) for initialization.
    This is also necessary because information about available AXS tables is persisted in a special AXS table in Spark
    metastore DB.
    """

    ZONE_HEIGHT = Constants.ONE_AMIN
    NGBR_BORDER_HEIGHT = 10 * Constants.ONE_ASEC
    RA_COLNAME = "ra"
    DEC_COLNAME = "dec"
    DUP_COLNAME = "dup"
    ZONE_COLNAME = "zone"

    NUM_BUCKETS = Constants.NUM_BUCKETS

    __instance = None

    def __init__(self, spark_session=None):
        """
        Initializes the catalog using connection to the Spark metastore of the current `SparkSession`.

        :param sparkSession: An active and initialized Spark session which can be used for accessing
        Spark catalog. Can be None if AxsCatalog has already been initialized at least once.
        """
        if spark_session is None and AxsCatalog.__instance is None:
            raise AttributeError("Spark session is none but AxsCatalog hasn't been initialized yet.")
        if spark_session is None:
            self.spark = AxsCatalog.__instance.spark
        else:
            self.spark = spark_session
        # _CatalogUtils is a bridge to AXS Java functions
        self._CatalogUtils = self.spark.sparkContext._jvm.org.dirac.axs.util.CatalogUtils()
        self._CatalogUtils.setup(self.spark._jsparkSession)
        AxsCatalog.__instance = self

    def load(self, table_name):
        """
        Loads a known AXS table from a Spark catalog and returns it as an `AxsFrame`.
        """
        # if table_name not in AxsCatalog._AXS_TABLES:
        info = self.table_info(table_name)

        table = self.spark.read.table(table_name)
        return AxsFrame(table, info)

    def import_existing_table(self, table_name, path, num_buckets=500, zone_height=Constants.ONE_AMIN,
            import_into_spark=True, update_spark_bucketing=True, bucket_col='zone', ra_col='ra', dec_col='dec',
            lightcurves=False, lc_cols=None):
        """
        Imports an existing, properly bucketed and sorted Parquet file into AXS catalog.
        If `import_into_spark` is True, the table will also be imported into Spark catalog.

        `bucket_col`, `ra_col` and `dec_col` allow for changing the default bucketing and sorting columns.

        If `lightcurves` is True, then some columns are expected to be array columns. Those are specified in `lc_cols`.
        array.

        :param table_name: The table name into which to import the data.
        :param path: The path to the bucketed Parquet file to be imported (needed only for Spark import).
        :param num_buckets: Number of buckets in the input Parquet file.
        :param zone_height: Zone height used for data partitioning.
        :param import_into_spark: Whether to also import the table into Spark. If `False`, the table should
            already exist in Spark metastore.
        :param update_spark_bucketing: Whether to also update bucketing info in Spark metastore.
        :param bucket_col: The column used for data bucketing.
        :param ra_col: The name of column containing RA coordinates.
        :param dec_col: The name of column containing DEC coordinates.
        :param lightcurves: Whether the table contains lightcurve data as array columns.
        :param lc_cols: Comma-separated list of names of array columns containing lightcurve data.
        """
        if self._CatalogUtils.tableExists(table_name):
            raise AttributeError("Table %s already exists in AXS catalog" % table_name)
        if import_into_spark:
            self.spark.catalog.createTable(table_name, path, "parquet")
        if update_spark_bucketing:
            self._CatalogUtils.updateSparkMetastoreBucketing(table_name, num_buckets)
        self._CatalogUtils.saveNewTable(table_name, num_buckets, zone_height,
                                        bucket_col, ra_col, dec_col, lightcurves, lc_cols)

    def table_info(self, table_name):
        """
        Returns a known AxsFrame table info as a dictionary with these keys:
        - `table_id` - Internal table ID
        - `table_name` - Name of the table
        - `num_buckets` - Number of buckets used for partitioning the table data
        - `zone_height` - Zone height used for data partitioning
        - `bucket_col` - the column name used for bucketing
        - `ra_col` - the column containing RA coordinates
        - `dec_col` - the column containing DEC coordinates
        - `has_lightcurves` - whether the table contains lightcurve data as array columns
        - `lc_columns` - a list of array columns containing lightcurve data

        :param table_name: Table name for which to fetch info.
        """
        # return AxsCatalog._AXS_TABLES
        tbls = self._CatalogUtils.listTables()
        for x in tbls:
            if x.getTableName() == table_name:
                return {'table_id': x.getTableId(), 'table_name': x.getTableName(),
                    'num_buckets': x.getNumBuckets(), 'zone_height': x.getZoneHeight(),
                    'bucket_col': x.getBucketCol(), 'ra_col': x.getRaCol(),
                    'dec_col': x.getDecCol(), 'has_lightcurves': x.isLightcurves(), 'lc_columns': x.getLcColumns()}
        raise AttributeError("Table %s not found in AXS catalog" % table_name)

    def list_tables(self):
        """
        Returns a list of a known AxsFrame tables as a dictionary where the keys are table names and the values
        are again dictionaries with these fields:
        - `table_id` - Internal table ID
        - `table_name` - Name of the table
        - `num_buckets` - Number of buckets used for partitioning the table data
        - `zone_height` - Zone height used for data partitioning
        - `bucket_col` - the column name used for bucketing
        - `ra_col` - the column containing RA coordinates
        - `dec_col` - the column containing DEC coordinates
        - `has_lightcurves` - whether the table contains lightcurve data as array columns
        - `lc_columns` - a list of array columns containing lightcurve data
        """
        tbls = self._CatalogUtils.listTables()
        res = {}
        for x in tbls:
            res[x.getTableName()] = {'table_id': x.getTableId(), 'table_name': x.getTableName(),
                'num_buckets': x.getNumBuckets(), 'zone_height': x.getZoneHeight(), 'bucket_col': x.getBucketCol(),
                'ra_col': x.getRaCol(), 'dec_col': x.getDecCol(), 'has_lightcurves': x.isLightcurves(),
                'lc_columns': x.getLcColumns()}
        return res

    def list_table_names(self):
        """
        Returns a list of a known AxsFrame table names.
        """
        tbls = self._CatalogUtils.listTables()
        res = []
        for x in tbls:
            res.append(x.getTableName())
        return res

    def save_axs_table(self, df, tblname, repartition=True, calculate_zone=False,
                       num_buckets=Constants.NUM_BUCKETS, zone_height=ZONE_HEIGHT,
                       path=None):
        """
        Saves a Spark DataFrame as an AxsFrame under the name `tblname`. Also saves the
        table as a Spark table in the Spark catalog. The table will be bucketed into
        `AxsCatalog.NUM_BUCKETS` buckets, each bucket sorted by `zone` and `ra` columns.

        Note: If saving intermediate results from cross-matching two AxsFrames the DataFrame should
        already be partitioned appropriately. `repartition` should then be set to `False`
        to speed things up.

        To obtained the saved table, use the `load()` method.

        :param df: Spark DataFrame or AxsFrame to save as AXS table.
        :param tblname: Table name to use for saving.
        :param repartition: Whether to repartition the data by zone before saving.
        :param calculate_zone: Whether to first add `zone` and `dup` columns to `df`.
        :param num_buckets: Number of buckets to use for data partitioning.
        :param path: Optional path under which to save the table data
        """
        # if tblname in AxsCatalog._AXS_TABLES:
        if self._CatalogUtils.tableExists(tblname):
            raise Exception("Table already exists: " + tblname)
        if AxsCatalog.DEC_COLNAME not in df.columns or AxsCatalog.RA_COLNAME not in df.columns:
            raise Exception("Cannot save as AXS table: '"+AxsCatalog.DEC_COLNAME+
                            "' or '"+AxsCatalog.RA_COLNAME+"' columns not found.")
        if AxsCatalog.ZONE_COLNAME not in df.columns and not calculate_zone:
            raise Exception("Cannot save as AXS table: '"+AxsCatalog.ZONE_COLNAME+
                            "' column not found and calculate_zone is not set.")

        old_partitions_conf = df.sql_ctx.getConf("spark.sql.shuffle.partitions")
        df.sql_ctx.setConf("spark.sql.shuffle.partitions", num_buckets)

        newdf = df
        if calculate_zone:
            newdf = self.calculate_zone(newdf, zone_height)

        if repartition:
            newdf = newdf.repartition("zone")

        writer = newdf.write.format("parquet"). \
            bucketBy(num_buckets, "zone").sortBy("zone", "ra")

        if path is not None:
            writer.option("path", path)

        writer.saveAsTable(tblname)

        # this modifies bucketing information in the Spark metastore
        self._CatalogUtils.saveNewTable(tblname, num_buckets, zone_height, AxsCatalog.ZONE_COLNAME,
                                                 AxsCatalog.RA_COLNAME, AxsCatalog.DEC_COLNAME,
                                                 False, None)
        df.sql_ctx.setConf("spark.sql.shuffle.partitions", old_partitions_conf)

    def calculate_zone(self, df, zone_height=ZONE_HEIGHT):
        """
        Adds `zone` and `dup` columns to the `df` DataFrame. `df` needs to have a `dec` column
        for calculating zones and must not already have `zone` and `dup` columns.
        Data in the lower border strip of each zone is duplicated to the zone below it. `dup` column
        of those rows contains 1 and 0 otherwise.

        :param df: The input DataFrame for which to calculate
        :param zone_height: Zone height to be used for data partitioning
        :return: The new AxsFrame
        """
        if AxsCatalog.ZONE_COLNAME in df.columns:
            raise Exception("Cannot save as AXS table: '" + AxsCatalog.ZONE_COLNAME +
                            "' column already exists.")
        if AxsCatalog.DUP_COLNAME in df.columns:
            raise Exception("Cannot save as AXS table: '"+AxsCatalog.DUP_COLNAME+"' column already exists")

        return df.where(((df.dec + 90) > zone_height) & (
                (df.dec + 90) % zone_height < AxsCatalog.NGBR_BORDER_HEIGHT)).\
                withColumn("zone", ((df.dec + 90) / zone_height - 1).cast("long")). \
                withColumn("dup", F.lit(1)).\
            union(df.withColumn("zone", ((df.dec + 90) / zone_height).cast("long")).
                withColumn("dup", F.lit(0)))

    def add_increment(self, table_name, increment_df, rename_to=None, temp_tbl_name=None):
        """
        Adds a new batch of data contained in the `increment_df` DataFrame (or AxsFrame) to the
        persisted AXS table `table_name`. The old table will be renamed to `rename_to`, if set, or to "`table_name`
        + _YYYYMMDDhhmm" otherwise.
        The data will be first saved into `temp_tbl_name` before renaming the main table.

        `increment_df` needs to have the same schema as the table `table_name`.

        :param table_name: The table to which to add the new data.
        :param increment_df: The data to add to the existing table. Needs to have the appropriate schema.
        :param rename_to: New table name for the original data.
        :param temp_tbl_name: Temporary table name to use (`table_name` + "_temp" is the default) before rename operation.
        :return: The table name to which the original table has been renamed
        """
        if temp_tbl_name is None:
            temp_tbl_name = table_name + "_temp"
        if rename_to is None:
            import datetime
            ts = datetime.datetime.now().strftime("%Y%m%d%H%M")
            rename_to = table_name + "_" + ts

        old = self.load(table_name)
        old = old.where(old.dup == 0)

        self.save_axs_table(old.union(self.calculate_zone(increment_df)), temp_tbl_name)

        self.rename_table(table_name, rename_to)
        self.rename_table(temp_tbl_name, table_name)

        self.drop_table(temp_tbl_name)
        return rename_to

    def rename_table(self, table_name, new_name):
        """
        Renames an AxsTable `table_name` to `new_name`. Also renames the table in the
        Spark catalog.

        :param table_name: An existing table to rename.
        :param new_name: The new name
        """
        # if new_name in AxsCatalog._AXS_TABLES:
        if self._CatalogUtils.tableExists(new_name):
            raise AttributeError("Table "+new_name+" already exists!")
        # if table_name not in AxsCatalog._AXS_TABLES:
        if not self._CatalogUtils.tableExists(table_name):
            raise AttributeError("Table "+table_name+" does not exist!")
        self._CatalogUtils.renameTable(table_name, new_name)
        # AxsCatalog._AXS_TABLES.remove(table_name)
        # AxsCatalog._AXS_TABLES.append(new_name)
        self.spark.sql("alter table " + table_name + " rename to " + new_name)

    def drop_table(self, table_name, drop_spark=True):
        """
        Drops a table from both AXS and Spark catalogs.

        :param table_name: Table to drop.
        :param drop_spark: Whether to drop the table in Spark catalog, too. Default is True.
        """
        try:
            self._CatalogUtils.deleteTable(table_name)
        except Exception as e:
            print(e)
        if drop_spark:
            try:
                self.spark.sql("drop table " + table_name)
            except Exception as e:
                print(e)
