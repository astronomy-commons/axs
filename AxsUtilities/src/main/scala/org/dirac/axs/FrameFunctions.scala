

package org.dirac.axs

import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

class FrameFunctions {

}

object FrameFunctions {
  def calcDist = (ra1: Double, ra2: Double, dec1: Double, dec2: Double) => {
    val delta_ra = Math.min(Math.abs(ra1 - ra2), 360 - (Math.abs(ra1 - ra2)))
    val delta_dec = dec1 - dec2
    val cosdec = Math.cos(dec1 * Math.PI / 180.0)
    Math.sqrt(Math.pow(cosdec, 2) * Math.pow(delta_ra, 2) + Math.pow(delta_dec, 2))
  }
  var calcDistUdf: Option[UserDefinedFunction] = None

  def crossmatch(ds1: Dataset[Row], ds2: Dataset[Row], r: Double, zoneHeight: Double,
                 useSMJOptim: Boolean = true): DataFrame = {
    val df1 = ds1.asInstanceOf[DataFrame]
    var df2 = ds2.asInstanceOf[DataFrame].withColumnRenamed("dup", "dup2")
    for (c <- df1.columns) {
      if (!c.equals("zone") && !c.equals("ra") && !c.equals("dec") && df2.columns.contains(c)) {
        df2 = df2.withColumnRenamed(c, "axs_"+c)
      }
    }

    if (calcDistUdf.isEmpty)
      calcDistUdf = Some(df1.sparkSession.udf.register("calcDist", calcDist))

    val join = if (useSMJOptim)
        df1.join(df2, df1("zone") === df2("zone") and (df1("ra")
        between(df2("ra") - (lit(r).divide(cos(lit(Math.PI/180.0)*(df2("zone")*zoneHeight - 90)))),
          df2("ra") + (lit(r).divide(cos(lit(Math.PI/180.0) * (df2("zone")*zoneHeight - 90))) ))))
      else
        df1.join(df2, df1("zone") === df2("zone") and (df1("ra") 
        between(df2("ra") - (lit(r).divide(cos(lit(Math.PI/180.0)*(df2("zone")*zoneHeight - 90)))),
          df2("ra") + (lit(r).divide(cos(lit(Math.PI/180.0) * (df2("zone")*zoneHeight - 90))) ))) 
        and (df1("dec") between(df2("dec") - r, df2("dec") + r)))

    val distcolname = "axsdist"

    val result = join.withColumn(distcolname, calcDistUdf.get(df1("ra"), df2("ra"), df1("dec"), df2("dec"))).
      withColumn("ratemp", df1("ra")).withColumn("dectemp", df1("dec")).withColumn("zonetemp", df1("zone")).
      drop("zone").drop("ra").drop("dec").
      withColumnRenamed("ratemp", "ra").
      withColumnRenamed("dectemp", "dec").
      withColumnRenamed("zonetemp", "zone").
      where((new Column(distcolname) lt r) and ((join("dup") === 0) or (join("dup2") === 0)))

    result
  }
}
