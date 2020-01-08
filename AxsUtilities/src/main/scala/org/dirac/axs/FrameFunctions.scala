

package org.dirac.axs

import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

class FrameFunctions {

}

object FrameFunctions {

  /** Calculate the distance between two ra, dec coordinate pairs.
   *
   *  Coordinates must be supplied in degrees, and the distance returned
   *  is in degrees.
   */
  def calcSeparation = (ra1: Double, ra2: Double, dec1: Double, dec2: Double) => {
    val delta_ra = Math.min(Math.abs(ra1 - ra2), 360 - (Math.abs(ra1 - ra2)))
    val delta_dec = Math.min(Math.abs(dec1 - dec2), 360 - (Math.abs(dec1 - dec2)))
    val cosdec = Math.cos(Math.toRadians(dec1))
    Math.sqrt(Math.pow(cosdec, 2) * Math.pow(delta_ra, 2) +
              Math.pow(delta_dec, 2))
  }

  var calcSeparationUDF: Option[UserDefinedFunction] = None

  /** Crossmatch two dataframes.
   *
   * @param ds1 an AxsFrame with the first dataset
   * @param ds2 an AxsFrame with the first dataset
   * @param matchRadius the maximum radius at which objects are associated, in degrees.
   * @param useSMJOptim use the Sort Merge Join optimization.
   * @param returnMin filter the results to only return the best matching object
   *                  from d2 for each object in d1.
   * @param include_dist include a column "axsdist" in the result, which is the
   *                     distance in degrees between matched objects.
   *
   */
  def crossmatch(ds1: Dataset[Row], ds2: Dataset[Row], matchRadius: Double, useSMJOptim: Boolean = true, returnMin: Boolean = false,
                 include_dist: Boolean = true): DataFrame = {
    val df1 = ds1.asInstanceOf[DataFrame]
    var df2 = ds2.asInstanceOf[DataFrame].withColumnRenamed("dup", "dup2")
    for (c <- df1.columns) {
      if (!c.equals("zone") && !c.equals("ra") && !c.equals("dec") && df2.columns.contains(c)) {
        df2 = df2.withColumnRenamed(c, "axs_"+c)
      }
    }

    if (calcSeparationUDF.isEmpty)
      calcSeparationUDF = Some(df1.sparkSession.udf.register("calcSeparation", calcSeparation))

    val join = if (useSMJOptim)
        df1.join(df2, df1("zone") === df2("zone") and
                      (df1("ra") between(df2("ra") - matchRadius, df2("ra") + matchRadius)))
      else
        df1.join(df2, df1("zone") === df2("zone") and
                      (df1("ra") between(df2("ra") - matchRadius, df2("ra") + matchRadius)) and
                      (df1("dec") between(df2("dec") - matchRadius, df2("dec") + matchRadius)))

    var distcolname = "axsdist"
    if (returnMin) {
      distcolname = "axsdisttemp"
    }
    val join2 = join.withColumn(distcolname, calcSeparationUDF.get(df1("ra"), df2("ra"), df1("dec"), df2("dec"))).
      withColumn("ratemp", df1("ra")).withColumn("dectemp", df1("dec")).withColumn("zonetemp", df1("zone")).
      drop("zone").drop("ra").drop("dec").
      withColumnRenamed("ratemp", "ra").
      withColumnRenamed("dectemp", "dec").
      withColumnRenamed("zonetemp", "zone").
      where((new Column(distcolname) lt matchRadius) and ((join("dup") === 0) or (join("dup2") === 0)))

    val res = if (returnMin) {

      val firstSels = join2.columns.map(c => if(c.equals(distcolname)) { first(join2.col(c)) as "axsdist" } else { first(join2.col(c)) as c })
      val w = Window.partitionBy(join2("zone"), join2("ra"), join2("dec")).orderBy(join2(distcolname))
      join2.withColumn("axsrownum", row_number().over(w)).where(col("axsrownum") === 1).drop("axsrownum").
        select(firstSels: _*)

    } else {
      join2
    }
    if (include_dist) {
      res
    } else {
      res.drop("axsdist")
    }
  }
}
