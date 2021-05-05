
import math
import numpy as np
import pandas as pd
from axs import Constants
from pyspark.sql import DataFrame, Window
from pyspark import _NoValue
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import pandas_udf, udf, spark_partition_id, PandasUDFType
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType


@pandas_udf(DoubleType())
def calc_dist_simple(r1in, d1in, r2in, d2in):
    import sys

    MAX_ANGLE = 2

    (r1, d1, r2, d2) = (r1in.values, d1in.values, r2in.values, d2in.values)
    # masking rows that are more than MAX_ANGLE apart (to avoid unnecessary computations)
    f = np.logical_or(np.logical_and(np.abs(r1 - r2) > MAX_ANGLE, np.abs(r1 - r2 - 360) > MAX_ANGLE),
                      np.abs(d1 - d2) > MAX_ANGLE)
    res = np.empty(len(r1))

    res[f] = sys.float_info.max

    # the opposite mask (these rows will be calculated)
    nf = np.logical_not(f)

    ra1 = r1[nf] / 180 * math.pi
    ra2 = r2[nf] / 180 * math.pi
    dec1 = d1[nf] / 180 * math.pi
    dec2 = d2[nf] / 180 * math.pi
    ra = np.minimum(np.abs(ra1 - ra2), 360 - np.abs(ra1 - ra2))
    dec = np.abs(dec1 - dec2)
    cosdec = np.cos(dec)
    res[nf] = np.sqrt(np.power(cosdec, 2) * np.power(np.sin(ra), 2) +
                      np.power(np.sin(dec), 2))
    return pd.Series(res)


@pandas_udf(DoubleType())
def dist_euclid(r1in, d1in, r2in, d2in):
    (r1, d1, r2, d2) = (r1in.values, d1in.values, r2in.values, d2in.values)
    rs = np.minimum(np.abs(r1 - r2), np.abs(r1 - r2 - 360))

    res = np.sqrt(np.power(rs, 2) + np.power(d2 - d1, 2))
    return pd.Series(res)


def create_min_udf(df, distcolname):
    schemadef = ", ".join([x.simpleString() for x in df.schema])
    @pandas_udf(schemadef, PandasUDFType.GROUPED_MAP)
    def min_distance(pdf):
        pdfmin = pd.DataFrame(
            pdf.loc[pdf[distcolname].idxmin()])
        return pdfmin.transpose()
    return min_distance


def dec_to_zone(dec, zone_height=Constants.ONE_AMIN):
    return int((dec+90)/zone_height)


def max_zone(zone_height=Constants.ONE_AMIN):
    return int(180/zone_height)


def wrap(df, table_info):
    return AxsFrame(df, table_info)


class AxsFrame(DataFrame):
    """
    AxsFrame is an extended Spark's DataFrame with additional methods useful for querying astronomical catalogs.
    It typically represents a Parquet Spark table containing properly bucketed (and sorted) data from an astronomical
    catalog, or a subset of such table.

    All Spark `DataFrame` methods which normaly return a `DataFrame` (such as `select` or `withColumn`) will return
    an `AxsFrame` when called on an `AxsFrame` instance.

    .. note:: One caveat, which results from AXS' partitioning and indexing scheme, is that you always need to make sure
        that you keep the `zone` and `ra` columns in AXS frames that you wish to later save or cross-match with
        other frames.
    """
    def __init__(self, dataframe, table_info):
        if not isinstance(dataframe, DataFrame):
            raise AttributeError('The object is not a Spark DataFrame')
        self._df = dataframe
        self._table_info = table_info
        self._support_repr_html = False

    _support_repr_html = False

    __ff = None

    def __get_ff(scin):
        AxsFrame.__ff
        if AxsFrame.__ff is None:
            AxsFrame.__ff = scin._jvm.org.dirac.axs.FrameFunctions()
        return AxsFrame.__ff

    def __repr__(self):
        return "AxsFrame[%s]" % (", ".join("%s: %s" % c for c in self.dtypes))

    def _repr_html_(self):
        return self.__repr__()

    def get_table_info(self):
        return self._table_info

    def crossmatch(self, axsframe, r=Constants.ONE_ASEC, return_min=False, include_dist_col=True):
        """
        Performs the cross-match operation between this AxsFrame and `axsframe`, which can be either an AxsFrame or
        a Spark's DataFrame, using `r` for the cross-matching radius (one arc-second by default).

        Both frames need to have `zone`, `ra`, `dec`, and `dup` columns.

        Bote that if `axsframe` is a Spark frame, the cross-match operation will not be optimized and might take
        a much longer time to complete.

        The best performance can be expected when both tables are read directly as AxsFrames.
        In that scenario cross-matching will be done on bucket pairs in parallel without data movement between
        executors. If, however, one of the two AxsFrames being cross-matched is the result of a `groupBy` operation,
        for example, data movement cannot be avoided. In those cases, it might prove faster to first save the
        "grouped by" table using `save_axs_table` and then do the cross-match.

        .. warning::
            The two AXS tables being cross-matched need to have the same zone height and the same number of buckets.
            The results are otherwise unreliable.

        The resulting table will contain all colunms from both input tables.

        :param axsframe: The AXS frame or Spark DataFrame to be used cross-matched
        :param r: The search radius used for cross-matching. One arc-second by default.
        :param return_min: Whether to return only a single cross-match result per row, or all the matching objects
        within the specified cross-match radius.
        :param include_dist_col: Whether to include the calculated distance column ("axsdist") in the results.
        Default is True.
        :return: AxsFrame containing cross-matching results with all columns from both frames.
        """
        t = type(axsframe)
        if t is not AxsFrame and t is not DataFrame:
            raise(AttributeError("Only AxsFrame and Spark DataFrame objects are supported for cross-match."))
        use_smj_optim = t is AxsFrame
        if use_smj_optim:
            if self._table_info['num_buckets'] != axsframe._table_info['num_buckets']:
                print("\nWARNING: the two tables don't have the same number of buckets. "
                      "Fast crossmatch is not possible. "
                      "Switching to the slower crossmatch algorithm version.\n")
                use_smj_optim = False
            if int(self._table_info['zone_height'] * 1e10)/1e10 != int(axsframe._table_info['zone_height'] * 1e10)/1e10:
                print("\nWARNING: the two tables don't have the same zone height. "
                      "Fast crossmatch is not possible. "
                      "Switching to the slower crossmatch algorithm version.\n")
                use_smj_optim = False

        ff = AxsFrame.__get_ff(self._sc)
        zone_height = self._table_info['zone_height']
        res = DataFrame(ff.crossmatch(self._jdf, axsframe._jdf, r, zone_height, use_smj_optim),
                        self.sql_ctx)

        if return_min:
            w = Window.partitionBy(res['zone'], res['ra'], res['dec']).orderBy(res['axsdist'])
            res = res.withColumn('axsrownum', F.row_number().over(w)).where("axsrownum == 1").drop('axsrownum')

        if not include_dist_col:
            res = res.drop(res['axsdist'])
        return wrap(res, self._table_info)

    def region(self, ra1=0, dec1=-90, ra2=360, dec2=90, spans_prime_mer=False):
        """
        Selects only the rows containing objects with RA between `ra1` and `ra2` and Dec between `dec1` and `dec2`.

        If `spans_prime_mer` is set to `True`, RA will be selected as ( >= `ra1` OR <= `ra2`).

        :param ra1: Lower RA bound.
        :param dec1: Lower Dec bound.
        :param ra2: Upper RA bound.
        :param dec2: Upper Dec bound.
        :param spans_prime_mer: Whether RA band spans the prime meridian (0 deg).
        :return: The AxsFrame containing the resulting data.
        """
        zone1 = dec_to_zone(dec1)
        zone2 = dec_to_zone(dec2)
        if spans_prime_mer:
            return wrap(self._df.where(self._df.zone.between(zone1, zone2) &
                                       (self._df.ra >= ra1 |
                                        self._df.ra <= ra2) &
                                       self._df.dec.between(dec1, dec2)), self._table_info)
        else:
            return wrap(self._df.where(self._df.zone.between(zone1, zone2) &
                                       self._df.ra.between(ra1, ra2) &
                                       self._df.dec.between(dec1, dec2)), self._table_info)

    def cone(self, ra, dec, r, remove_duplicates=False):
        """
        Selects the rows containing objects falling into a 'cone' defined by `ra` and `dec` center point
        and the radius `r`.

        The function still doesn't handle the situation when the cone overlaps one of the poles. In that case
        maximum and minimum dec coordinates are clipped at the poles.

        :param ra: Ra of the center point. Has to be inside [0, 360).
        :param dec: Dec of the center point. Has to be inside [-90, 90].
        :param r: Radius of the search cone in degrees. Has to be inside (0, 90].
        :param remove_duplicates: If True, duplicated rows will be removed from the resulting AxsFrame.
        If you plan to crossmatch the results with another AxsFrame, leave this at False.
        :return: The AxsFrame containing the rows found inside the search cone.
        """
        if r <= 0 or r > 90:
            raise ValueError("Radius has to be inside (0, 90] interval. Got: "+str(r))
        if ra < 0 or ra >= 360:
            raise ValueError("Ra has to be inside [0, 360) interval. Got: "+str(ra))
        if dec < -90 or dec > 90:
            raise ValueError("Dec has to be inside [-90, 90] interval. Got: "+str(dec))
        zone1 = dec_to_zone(dec - r)
        zone2 = dec_to_zone(dec + r)
        if zone1 < 0:
            zone1 = 0
        if zone2 > max_zone():
            zone2 = max_zone()
        dec1 = dec - r
        dec2 = dec + r
        if dec1 < -90:
            dec1 = -90
        if dec2 > 90:
            dec2 = 90

        ra1extra = -1
        ra2extra = -1
        ra1 = ra - r
        ra2 = ra + r

        if ra1 < 0:
            ra1extra = 360 + ra1
            ra2extra = 360
            ra1 = 0
        if ra2 > 360:
            ra1extra = 0
            ra2extra = ra2 - 360
            ra2 = 360

        if ra1extra >= 0:
            res = wrap(self._df.where(self._df.zone.between(zone1, zone2) &
                                (self._df.ra.between(ra1, ra2) | self._df.ra.between(ra1extra, ra2extra)) &
                                self._df.dec.between(dec1, dec2) &
                                (dist_euclid(F.lit(ra), F.lit(dec), self._df.ra, self._df.dec) <= r)), self._table_info)
        else:
            res = wrap(self._df.where(self._df.zone.between(zone1, zone2) &
                                self._df.ra.between(ra1, ra2) &
                                self._df.dec.between(dec1, dec2) &
                                (dist_euclid(F.lit(ra), F.lit(dec), self._df.ra, self._df.dec) <= r)), self._table_info)
        if remove_duplicates:
            res = res.exclude_duplicates()
        return res

    def histogram(self, cond, numbins):
        """
        Uses `cond` column expression to obtain data for histogram calculation. The data will be binned into
        `numbins` bins.

        :param cond: Column expression determining the data.
        :param numbins: Number of bins.
        :return: AxsFrame containing row counts per bin.
        """
        colname = "axs_hist_col"
        res = self._df.select(cond.alias(colname))
        return res.withColumn("bin", (res[colname] / numbins).cast("int")).groupBy("bin").count()

    def histogram2d(self, cond1, cond2, numbins1, numbins2, min1=None, max1=None, min2=None, max2=None):
        """
        Uses `cond1` and `cond2` colunm expressions to obtain data for 2D histogram calculation. The data on
        x axis will be binned into `numbins1` bins. The data on y axis will be binned into `numbins2` bins.
        If `min1`, `max1`, `min2` or `max2` are not spacified, they will be calculated using an additional pass
        through the data.
        The method returns x, y and z 2-D numpy arrays (see numpy.mgrid) which can be used as an input to
        `matplotlib.pcolormesh`.

        :param cond1: Column expression determining the data on x axis.
        :param cond2: Column expression determining the data on y axis.
        :param numbins1: Number of bins for x axis.
        :param numbins2: Number of bins for y axis.
        :param min1: Optional minimum value for x axis data.
        :param max1: Optional maximum value for x axis data.
        :param min2: Optional minimum value for y axis data.
        :param max2: Optional maximum value for y axis data.
        :return: x, y, z 2-D numpy "meshgrid" arrays (see numpy.mgrid)
        """
        colname1 = "axs_hist_col1"
        colname2 = "axs_hist_col2"
        res = self._df.select(cond1.alias(colname1), cond2.alias(colname2))

        if min1 is None or max1 is None or min2 is None or max2 is None:
            mm = res.select(F.min(res[colname1]).alias("min1"), F.max(res[colname1]).alias("max1"),
                            F.min(res[colname2]).alias("min2"), F.max(res[colname2]).alias("max2")).\
                collect()
            (min1, max1, min2, max2) = (mm[0]["min1"], mm[0]["max1"], mm[0]["min2"], mm[0]["max2"])

        rng1 = float(max1 - min1)
        rng2 = float(max2 - min2)
        step1 = rng1 / numbins1
        step2 = rng2 / numbins2

        hist2d = res.withColumn("bin1", ((res[colname1]-min1)/step1).cast("int")) \
                    .withColumn("bin2", ((res[colname2]-min2)/step2).cast("int")) \
                    .groupBy("bin1", "bin2").count()
        hist2data = hist2d.orderBy(hist2d.bin1, hist2d.bin2).collect()
        bin1 = np.array(list(map(lambda row: row.bin1, hist2data)))
        bin2 = np.array(list(map(lambda row: row.bin2, hist2data)))
        vals = np.array(list(map(lambda row: row["count"], hist2data)))

        x, y = np.mgrid[slice(min1, max1 + step1, step1),
                        slice(min2, max2 + step2, step2)]

        z = np.zeros(numbins1*numbins2)
        ok_bins = np.where((bin1 >= 0) & (bin1 < numbins1) & (bin2 >= 0) & (bin2 < numbins2))
        bin_onedim_index = bin2 + bin1*numbins2
        z[bin_onedim_index[ok_bins]] = vals[ok_bins]

        return x, y, z.reshape((numbins1, numbins2))

    def healpix_hist(NSIDE=64, groupby=[], agg={"*": "count"}, healpix_column="hpix12",
                     return_df=False):
        """
        Return a healpix-binned histogram at a resolution specified by NSIDE,
        and after performing aggregations specified in agg.

        :param NSIDE: Resolution of the output healpix map, default NSIDE=64.
        :param groupby: List of columns to group by, either as strings or Spark column objects.
        :param agg: Dictionary of aggregations to apply, passed to Spark's agg function.
        :param healpix_column: Column name containing healpix pixel ID values.
        :param return_df: Return a dataframe with the aggregated data, not reshaped into a healpix map.
        :return: Array of aggregated values in the shape of a healpix map (unless return_df=True)
        """

        from pyspark.sql.functions import floor as FLOOR, col as COL, lit, shiftRight

        order0 = 12
        order  = hp.nside2order(NSIDE)
        shr    = 2*(order0 - order)

        # construct query
        df = self._df.withColumn('hpix__', shiftRight(healpix_column, shr))
        gbcols = ('hpix__', )
        for axspec in groupby:
            if not isinstance(axspec, str):
                (col, c0, c1, dc) = axspec
                df = ( df
                    .where((lit(c0) < COL(col)) & (COL(col) < lit(c1)))
                    .withColumn(col + '_bin__', FLOOR((COL(col) - lit(c0)) / lit(dc)) * lit(dc) + lit(c0) )
                     )
                gbcols += ( col + '_bin__', )
            else:
                gbcols += ( axspec, )
        df = df.groupBy(*gbcols)

        # execute aggregation
        df = df.agg(agg)

        # fetch result
        df = df.toPandas()
        if returnDf:
            return df

        # repack the result into maps
        # This results line is slightly dangerous, because some aggregate functions are purely aliases.
        # E.g., mean(x) gets returned as a column avg(x).
        results = [ f"{v}({k})" if k != "*" else f"{v}(1)" for k, v in agg.items() ]    # Result columns
        def _create_map(df):
            maps = dict()
            for val in results:
                map_ = np.zeros(hp.nside2npix(NSIDE))
                # I think this line throws an error if there are no rows in the result
                map_[df.hpix__.values] = df[val].values
                maps[val] = [ map_ ]
            return pd.DataFrame(data=maps)

        idxcols = list(gbcols[1:])
        if len(idxcols) == 0:
            ret = _create_map(df)
            assert(len(ret) == 1)
            if not returnDf:
                # convert to tuple, or scalar
                ret = tuple(ret[name].values[0] for name in results)
                if len(ret) == 1:
                    ret = ret[0]
        else:
            ret = df.groupby(idxcols).apply(_create_map)
            ret.index = ret.index.droplevel(-1)
            ret.index.rename([ name.split("_bin__")[0] for name in ret.index.names ], inplace=True)
            if "count(1)" in ret:
                        ret = ret.rename(columns={'count(1)': 'count'})
            if not returnDf:
                if len(ret.columns) == 1:
                    ret = ret.iloc[:, 0]
        return ret

    def exclude_duplicates(self):
        """
        Removes the duplicated data (where `dup` is equal to `1`) from the AxsFrame.

        The AxsFrame needs to contain the `dup` column.

        :return: The AxsFrame with duplicated data removed.
        """
        from axs.catalog import AxsCatalog
        return self.filter(self._df[AxsCatalog.DUP_COLNAME] == 0)

    def save_axs_table(self, tblname, calculate_zone=False):
        """
        Persists the `AxsFrame` under the name `tblname` to make it available for loading in the future.
        The table will be available under this name to all Spark applications using the same metastore.

        If `calculate_zone` is set to `True`, the `zone` column used for bucketing the data must not exist as it
        will be added before saving. If `calculate_zone` is `True`, the `dup` column will also be added and the
        data from border stripes in neighboring zones will be duplicated to the zone below.
        If `calculate_zone` is `False`, the `zone` and `dup` columns need to already be present.

        The AxsFrame also needs to have the `ra` column because it will be used for data indexing along with the
        `zone` column.

        See also `catalog.save_axs_table()`.

        :param tblname: Table name to use for persisting.
        :param calculate_zone: Whether to calculate the `zone` column.
        """
        from axs.catalog import AxsCatalog
        AxsCatalog(None).save_axs_table(self, tblname, True, calculate_zone)

    def add_primitive_column(self, colname, coltype, func, *col_names):
        """
        Add a column `colname` of type `coltype` to this `AxsFrame` object by executing function `func` on
        columns `col_names`. Function `func` has to accept the exact number of arguments as the number of columns
        specified. The arguments will be of type Pandas.Series and the function needs to return a Pandas.Series
        object of type represented by string `coltype`.

        This method will use `@pandas_udf` in the background (see `pyspark.functions.pandas_udf`).

        The type of the new column needs to be a primitive type (int, double, etc.). The input columns can be of
        complex types.

        This function is faster than `add_column` but cannot *produce* array or map columns.

        :param colname: The name of the new column.
        :param coltype: The name of the new column's type. (See `pyspark.sql.types`)
        :param func: The function which will produce data for the new column. Needs to operate on Pandas.Series objects
        (of the same number as the number of `col_names`) and return a single Pandas.Series object of the type defined
        by `coltype`.
        :param col_names: The columns whose data will be supplied to the function `func`.
        :return: Returns a new AxsFrame with the new column added.
        """
        from pyspark.sql.types import _parse_datatype_string
        t = _parse_datatype_string(coltype)

        @pandas_udf(t)
        def pudf(*cols):
            return func(*cols)
        columns = [self._df[cn] for cn in col_names]
        return wrap(self._df.withColumn(colname, pudf(*columns)), self._table_info)

    def add_column(self, colname, coltype, func, *col_names):
        """
        Add a column `colname` of type `coltype` to this `AxsFrame` object by executing function `func` on
        columns `col_names`. Function `func` has to accept the exact number of arguments as the number of columns
        specified. The function will be applied to the dataframe data row by row. The arguments passed to the function
        will match types of the specified columns and the function needs to return an object of type represented by
        string `coltype`.

        This method will use `@udf` in the background (see `pyspark.functions.udf`).

        This function is slower than `add_column` but can produce array or map columns.

        :param colname: The name of the new column.
        :param coltype: The name of the new column's type. (See `pyspark.sql.types`)
        :param func: The function which will produce data for the new column. Needs to return a single object of the
        type defined by `coltype`.
        :param col_names: The columns whose data will be supplied to the function `func`.
        :return: Returns a new AxsFrame with the new column added.
        """
        from pyspark.sql.types import _parse_datatype_string
        t = _parse_datatype_string(coltype)

        @udf(returnType=t)
        def customudf(*cols):
            return func(*cols)

        columns = [self._df[cn] for cn in col_names]
        return wrap(self._df.withColumn(colname, customudf(*columns)), self._table_info)

    #####################
    # Wrapped methods from Spark DataFrame
    #####################

    @property
    def _jdf(self):
        return self._df.__dict__['_jdf']

    @property
    def sql_ctx(self):
        return self._df.__dict__['sql_ctx']

    @property
    def _sc(self):
        return self._df.__dict__['_sc']

    @property
    def _schema(self):
        return self._df.__dict__['_schema']

    def withColumn(self, colName, col):
        return wrap(self._df.withColumn(colName, col), self._table_info)

    def drop(self, *cols):
        return wrap(self._df.drop(*cols), self._table_info)

    def show(self, n=20, truncate=True, vertical=False):
        self._df.show(n, truncate, vertical)

    def count(self):
        return self._df.count()

    @property
    def rdd(self):
        return self._df.rdd

    @property
    def na(self):
        return self._df.na

    @property
    def stat(self):
        return self._df.stat

    def toJSON(self, use_unicode=True):
        return self._df.toJSON(use_unicode)

    def registerTempTable(self, name):
        self._df.registerTempTable(name)

    def createTempView(self, name):
        self._df.createTempView(name)

    def createOrReplaceTempView(self, name):
        self._df.createOrReplaceTempView(name)

    def createGlobalTempView(self, name):
        self._df.createGlobalTempView(name)

    def createOrReplaceGlobalTempView(self, name):
        self._df.createOrReplaceGlobalTempView(name)

    @property
    def write(self):
        return self._df.write

    @property
    def writeStream(self):
        return self._df.writeStream

    @property
    def schema(self):
        return self._df.schema

    def printSchema(self):
        self._df.printSchema()

    def explain(self, extended=False):
        self._df.explain(extended)

    def isLocal(self):
        return self._df.isLocal()

    @property
    def isStreaming(self):
        return self._df.isStreaming

    def checkpoint(self, eager=True):
        return self._df.checkpoint(eager)

    def collect(self):
        return self._df.collect()

    def limit(self, num):
        return self._df.limit(num)

    def take(self, num):
        return self._df.take(num)

    def foreach(self, f):
        self._df.foreach(f)

    def foreachPartition(self, f):
        self._df.foreachPartition(f)

    def cache(self):
        self._df.cache()
        return self

    def persist(self, storageLevel=StorageLevel.MEMORY_AND_DISK):
        self._df.persist(storageLevel)
        return self

    def unpersist(self, blocking=False):
        self._df.unpersist(blocking)
        return self

    def coalesce(self, numPartitions):
        return wrap(self._df.coalesce(numPartitions), self._table_info)

    def distinct(self):
        return wrap(self._df.distinct(), self._table_info)

    def sample(self, withReplacement=None, fraction=None, seed=None):
        return wrap(self._df.sample(withReplacement, fraction, seed), self._table_info)

    def sampleBy(self, col, fractions, seed=None):
        return wrap(self._df.sampleBy(col, fractions, seed), self._table_info)

    def randomSplit(self, weights, seed=None):
        return [wrap(x, self._table_info) for x in self._df.randomSplit(weights, seed)]

    @property
    def dtypes(self):
        return self._df.dtypes

    @property
    def columns(self):
        return self._df.columns

    def colRegex(self, colName):
        return self._df.colRegex(colName)

    def alias(self, alias):
        return wrap(self._df.alias(alias), self._table_info)

    def join(self, other, on=None, how=None):
        return wrap(self._df.join(other, on, how), self._table_info)

    def sortWithinPartitions(self, *cols, **kwargs):
        return wrap(self._df.sortWithinPartitions(*cols, **kwargs), self._table_info)

    def sort(self, *cols, **kwargs):
        return wrap(self._df.sort(*cols, **kwargs), self._table_info)

    def describe(self, *cols):
        return self._df.describe(*cols)

    def head(self, n=None):
        return self._df.head(n)

    def first(self):
        return self._df.first()

    def select(self, *cols):
        return wrap(self._df.select(*cols), self._table_info)

    def selectExpr(self, *expr):
        return wrap(self._df.selectExpr(*expr), self._table_info)

    def filter(self, condition):
        from axs.catalog import AxsCatalog
        if AxsCatalog.DUP_COLNAME in self._df.columns:
            return wrap(self._df.filter(condition), self._table_info)
        return wrap(self._df.filter(condition), self._table_info)

    def where(self, condition):
        return self.filter(condition)

    def groupBy(self, *cols):
        return self._df.groupBy(*cols)

    def groupby(self, *cols):
        return self.groupBy(*cols)

    def rollup(self, *cols):
        return self._df.rollup(*cols)

    def cube(self, *cols):
        return self._df.cube(*cols)

    def agg(self, *exprs):
        return self._df.agg(*exprs)

    def union(self, other):
        return wrap(self._df.union(other), self._table_info)

    def unionAll(self, other):
        return wrap(self._df.unionAll(other), self._table_info)

    def unionByName(self, other):
        return wrap(self._df.unionByName(other), self._table_info)

    def intersect(self, other):
        return wrap(self._df.intersect(other), self._table_info)

    def subtract(self, other):
        return wrap(self._df.subtract(other), self._table_info)

    def dropDuplicates(self, subset=None):
        return wrap(self._df.dropDuplicates(subset), self._table_info)

    def dropna(self, how='any', thresh=None, subset=None):
        return wrap(self._df.dropna(how, thresh, subset), self._table_info)

    def fillna(self, value, subset=None):
        return wrap(self._df.fillna(value, subset), self._table_info)

    def replace(self, to_replace, value=_NoValue, subset=None):
        return wrap(self._df.replace(to_replace, value, subset), self._table_info)

    def approxQuantile(self, col, probabilities, relativeError):
        return self._df.approxQuantile(col, probabilities, relativeError)

    def corr(self, col1, col2, method=None):
        return self._df.corr(col1, col2, method)

    def cov(self, col1, col2):
        return self._df.cov(col1, col2)

    def crosstab(self, col1, col2):
        return wrap(self._df.crosstab(col1, col2), self._table_info)

    def freqItems(self, cols, support=None):
        return wrap(self._df.freqItems(cols, support), self._table_info)

    def withColumnRenamed(self, existing, new):
        return wrap(self._df.withColumnRenamed(existing, new), self._table_info)

    def toDF(self, *cols):
        return wrap(self._df.toDF(*cols), self._table_info)

    def toPandas(self):
        return self._df.toPandas()
