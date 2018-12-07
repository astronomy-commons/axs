
======================
Importing catalog data
======================

Using AXS without real catalog data is probably good for testing purposes, but you will probably want to load into AXS
some real astronomical measurements from catalogs such as SDSS or Gaia. There are several ways how to load data into AXS.
The goal here is to have your AXS catalog aware of where the data resides. AXS data is physically stored in Parquet files (as
was described in the :doc:`introduction <introduction>`, and the information about them is stored in the AXS "metastore".

AXS metastore relies on, and extends, Spark's metastore. By default, Spark (and AXS) will store metastore data (information
about tables, their physical location, bucketing information, and so on) in a Derby database, created in a folder called
`metastore_db` in your working folder. You can also specify a different database to be used as a metastore by placing
a `hive-site.xml` file, containing the required configuration, in the `conf` sub-directory of your AXS installation folder.
AXS installation contains an example of this file for connecting to a MariaDB.

So, the goal is to have catalog data properly partitioned in Parquet files, on a local or a distributed file system,
and information about the table data registered in the Spark metastore. Additionally, AXS also needs to register
the table data in its own configuration tables.

There are three ways you can accomplish this:

- importing data from downloaded Parquet files
- importing data already registered in Spark metastore
- importing new data from scratch

Importing data from Parquet files
=================================

You can download catalog data as pre-partitioned AXS Parquet files. After placing the files in a folder (for example
`/data/axs/sdss`), load the files into AXS metastore by calling `AxsCatalog`'s `import_existing_table` method:


.. code-block:: python

    from axs import AxsCatalog, Constants
    db = AxsCatalog(spark)
    db.import_existing_table('sdss_table', '/data/axs/sdss', num_buckets=500, zone_height=Constants.ONE_AMIN,
        import_into_spark=True)

This will register the files from the `/data/axs/sdss` folder as a new table (visible both from Spark and AXS)
called `sdss_table` and tell Spark that the files are bucketed by the `zone` column into 500 buckets and that the data
within the buckets is sorted by `zone` and `ra` columns. When using this method it is assumed that there is no table
with the same name in the metastore and that the Parquet files are indeed bucketed as described.

Importing a table from a Spark metastore
========================================

If you have a properly bucketed and AXS-readable table already registered in the Spark metastore as a Spark table, you
can use the same `AxsCatalog` method with different parameters to register the table in AXS metastore:

.. code-block:: python

    db.import_existing_table('sdss_table', '', num_buckets=500, zone_height=Constants.ONE_AMIN,
        import_into_spark=False, update_spark_bucketing=True)

The `path` argument is not used in this case because the path is known for an existing Spark table. The difference is
the value of `import_into_spark` parameter, which is now set to `False`. You can set `update_spark_bucketing` to `False`
if you know that Spark is aware of table's bucketing. If a properly bucketed Parquet file was imported into Spark using
Spark API, the bucketing information will not be available, so you will probably want to set `update_spark_bucketing`
to `True`.

Importing new data from scratch
===============================

Finally, you can also import new data into AXS and specify it as a Spark `DataFrame`. You use `AxsCatalog`'s `save_axs_table`
method for that, which looks like this:

.. code-block:: python

    def save_axs_table(self, df, tblname, repartition=True, calculate_zone=False,
                       num_buckets=Constants.NUM_BUCKETS, zone_height=ZONE_HEIGHT):

As a bare minimum, you need to provide a Spark `DataFrame` (variable `df`), containing at least `ra` and `dec` columns,
and the desired table name. You
would typically obtain the `DataFrame` by loading and parsing data from external files (e.g. FITS or HDF5 files). If the
data is already correctly partitioned (in Spark partitions), set `repartition` to `False`. If the `zone` column is already
present in the input `DataFrame`, set `calculate_zone` to `False`. Finally, it is best to leave the defaults for the
number of buckets and zone height, but you can also override the defaults.
