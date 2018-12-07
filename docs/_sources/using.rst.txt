
=======================
Using AXS
=======================

With AXS you can cross-match, query, and analyze data from astronomical catalogs. This page provides a brief
description of these features.

Cross-matching catalogs
=======================

After you have :doc:`installed AXS <installation>`, :doc:`started AXS <getting_started>`, and
:doc:`imported data <importing_data>` from at least two catalogs, you can cross-match them.

First, you can get the list of tables in the AXS metastore using `list_tables`. The following snippet, for example,
will print out the names of known tables:

.. code-block:: python

    db = AxsCatalog(spark)
    for tname in db.list_tables():
        print(tname)

To load tables, you use `AxsCatalog`'s `load` method:

.. code-block:: python

    sdss = db.load('sdss')
    gaia = db.load('gaia')

Both `sdss` and `gaia` are objects of type `AxsFrame`. You can cross-match the two (with 2 arc-second radius, as in the
next example) using the `crossmatch` method:

.. code-block:: python

    gaia_sdss_cm = gaia.crossmatch(sdss, 2*Constants.ONE_ASEC,
        return_min=False, include_dist_col=True)

If you set `return_min` to `True`, only the nearest match will be returned for each row. `include_dist_col` determines
if the calculated distance will be returned as one of the columns (named `axsdist`).

Now that you have cross-matching results in an `AxsFrame`, you can do further processing on the results using Spark
and AXS APIs.

Querying catalog data
=====================
To query and process AXS catalog data you can use standard Spark API. However, one important concept in AXS is data
duplication: some of the rows (those in border areas of each zone) are duplicated. Those rows will have the `dup` column
set to `1`. The `dup` column is used for quick cross-matching. But during normal data processing, this duplication is
unwanted. You can exlude the duplicated rows using `AxsFrame`'s `exclude_duplicates` function:

.. code-block:: python

    gaia_clean = gaia.exclude_duplicates()

AXS provides several additional astronomy-specific methods:

- `region` returns all objects from an `AxsFrame` inside a square specified by two points in the sky (and their 'ra' and 'dec' coordinates).
- `cone` returns all objects from an `AxsFrame` falling in a cone specified by its central point and a radius.
- With `histogram` you can specify a function (a new column definition) for calculating a value which will be used for binning the rows. You also need to set the number of bins to use.
- With `histogram2d` you can do the same thing in two dimensions.

Running custom functions
========================
Two methods deserve special mention: `add_primitive_column(colname, coltype, func, *col_names)` and
`add_column(colname, coltype, func, *col_names)`. They serve the same purpose: making it easier to run custom, user-defined
data processing functions. The user-defined function should run on a single row at a time and produce one value as a result.
`add_primitive_column` supports only output columns of primitive types (integer, float, string, and so on), but should
be faster to run (it uses Spark's `@pandas_udf` mechanism). `add_column` also supports complex columns (map, array, struct)
and uses Spark's `@udf` mechanism.

You have to provide the resulting column name and its type (`colname` and `coltype` arguments, respectively), the names of the
columns whose values should serve as an input to the function (`col_names` argument), and the function itself (`func`).

The function for `add_primitive_column` needs to operate on `Pandas.Series` objects (of the same number as the number of
`col_names`) and return a single `Pandas.Series` object of the type defined by `coltype`.

The function for `add_column` should be an ordinary Python function, returning a single object of the type defined by `coltype`.

Working with light-curve data
=============================
AXS stores light-curve data in array columns. The intention is to have one row per object and store the different
measurements as elements of arrays. For example, a star would have a row with its Ra and Dec coordinates, possibly
also an ID column, and 5 measurements of the star in these columns:

- `mjd` - containing times at which measurements were taken
- `filter` - containing filters that were used: `r`, `g`, `r`, etc.
- `mag` - containing the actual measurements

Other columns (such as `mag_err`) could also be used, depending on the actual catalog.

To query these data AXS relies on Spark functionalities and adds two additional SQL functions: `array_allpositions` and
`array_select`. `array_allpositions` takes an array column and a value and returns the positions of all the occurrences of the
value in the given array column as an array of longs. `array_select` takes an array column and an array of indices (which can
be another column or a value) and returns all elements from the column array corresponding to the provided indices.

These functions, enabling filtering of array columns, along with other Spark built-in functions should be adequate for
most use cases.


