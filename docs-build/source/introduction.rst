===================
Introduction to AXS
===================

AXS is a framework for processing data from astronomical catalogs based on `Apache Spark <https://spark.apache.org/>`__.
AXS stands for *Astronomy eXtensions for Spark*.

AXS was designed to make life easier for astronomers when they have to
cruch data from astronomical catalogs (which is close to "all the time"). The way this process usually works is that
an astronomer selects some data from a public or a private catalog using an SQL query, exports that data to some
location and then writes custom (most likely Python) scripts to further analyze the data. Relying on the underlying
Spark engine, AXS simplifies this process by allowing astronomers to query the data and run arbitrary processing
on the results *all within the same framework*. There is usually no need for data export and separate programs.

Another thing that astronomers often need to do is to cross-match several catalogs: find the same objects from
different catalogs based on distance calculations. This is computationally very expensive operation. AXS partitions
the data in a way that makes cross-matching extremely efficient.

Spark specifics
================

If you wish to use AXS and you have never worked with Spark, you will have some adjusting to do because AXS is a
wrapper around Spark, or Spark SQL to be exact. :class:`AxsFrame` class is a wrapper around Spark's `DataFrame` so
all operations available on `DataFrames` are also available on :class:`AxsFrame`.

There are a few concepts that a new Spark user might find confusing.

First of all, `DataFrames` (and `AxsFrames`) are *lazy* and will be evaluated and executed only when an *action*
(such as `show` or `count`) is called.

Secondly, `DataFrames` are immutable. When you call a *transformation* (such as `select` or `where`)  you obtain a new
`DataFrame`. Stacking up transformations builds the `DataFrame` "program" to be executed later.

Because of all this, if this program results in an error, you will have to examine all the transformations that
went into building the `DataFrame` and not just the last one (which would be the first, natural reaction).

Spark is a complex framework and execution engine and you would do well to invest some time into learning Spark basics
if you intend to use AXS.

AXS data partitioning
=====================

AXS partitions the sky into horizontal strips of fixed height, called *zones*. Data is physically partitioned into
*buckets*, which are ordinary (Parquet) files containing part of the data of the full dataset. Zones are sequentially placed
into buckets. Since there are many more zones than buckets, a single bucket contains data from various
non-neighboring zones. Within a bucket, data is sorted by zone (derived from the Dec coordinate) and then by the
RA coordinate.

This setup enables fast spatial searches and fast cross-matching of different catalogs.

