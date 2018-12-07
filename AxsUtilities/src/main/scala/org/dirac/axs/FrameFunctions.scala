

package org.dirac.axs

import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, Dataset, Row}

class FrameFunctions {

}

object FrameFunctions {
  def calcGnom = (r1: Double, r2: Double, d1: Double, d2: Double) => {
    val doNotCalc = (Math.abs(r1 - r2) > 1 && Math.abs(r1 - r2 - 360) > 1) || Math.abs(d1 - d2) > 1
    if (doNotCalc) {
      Double.MaxValue
    } else {
      val ra1 = r1 / 180 * Math.PI
      val ra2 = r2 / 180 * Math.PI
      val dec1 = d1 / 180 * Math.PI
      val dec2 = d2 / 180 * Math.PI
      val ra = Math.min(Math.abs(ra1 - ra2), 360 - (Math.abs(ra1 - ra2)))
      val dec = Math.min(Math.abs(dec1 - dec2), 360 - (Math.abs(dec1 - dec2)))
      val cosdec = Math.cos(dec)
      val cosc = Math.cos(ra) * cosdec
      Math.sqrt(Math.pow(cosdec, 2) * Math.pow(Math.sin(ra), 2) +
        Math.pow(Math.sin(dec), 2)) / cosc
    }
  }
  var calcGnomUdf: Option[UserDefinedFunction] = None

  def degToGnom(deg: Double) = {
    val d2 = deg / 180 * Math.PI
    Math.sin(d2) / Math.cos(d2)
  }

  def crossmatch(ds1: Dataset[Row], ds2: Dataset[Row], r: Double, useSMJOptim: Boolean = true, returnMin: Boolean = false,
                 include_dist: Boolean = true): DataFrame = {
    val df1 = ds1.asInstanceOf[DataFrame]
    var df2 = ds2.asInstanceOf[DataFrame].withColumnRenamed("dup", "dup2")
    for (c <- df1.columns) {
      if (!c.equals("zone") && !c.equals("ra") && !c.equals("dec") && df2.columns.contains(c)) {
        df2 = df2.withColumnRenamed(c, "axs_"+c)
      }
    }

    if (calcGnomUdf.isEmpty)
      calcGnomUdf = Some(df1.sparkSession.udf.register("calcGnom", calcGnom))

    val gnom_dist = degToGnom(r)

    val join = if (useSMJOptim)
        df1.join(df2, df1("zone") === df2("zone") and (df1("ra") between(df2("ra") - r, df2("ra") + r)))
      else
        df1.join(df2, df1("zone") === df2("zone") and (df1("ra") between(df2("ra") - r, df2("ra") + r)) and
          (df1("dec") between(df2("dec") - r, df2("dec") + r)))

    var distcolname = "axsdist"
    if (returnMin) {
      distcolname = "axsdisttemp"
    }
    val join2 = join.withColumn(distcolname, calcGnomUdf.get(df1("ra"), df2("ra"), df1("dec"), df2("dec"))).
      withColumn("ratemp", df1("ra")).withColumn("dectemp", df1("dec")).withColumn("zonetemp", df1("zone")).
      drop("zone").drop("ra").drop("dec").
      withColumnRenamed("ratemp", "ra").
      withColumnRenamed("dectemp", "dec").
      withColumnRenamed("zonetemp", "zone").
      where((new Column(distcolname) lt gnom_dist) and ((join("dup") === 0) or (join("dup2") === 0)))

    val res = if (returnMin) {
      //join2.registerTempTable("join2")

      val firstSels = join2.columns.map(c => if(c.equals(distcolname)) { first(join2.col(c)) as "axsdist" } else { first(join2.col(c)) as c })
      val w = Window.partitionBy(join2("zone"), join2("ra"), join2("dec")).orderBy(join2(distcolname))
      join2.withColumn("axsrownum", row_number().over(w)).where(col("axsrownum") === 1).drop("axsrownum").
        select(firstSels: _*)

      //val firstSels = join2.columns.map(c => if(c.equals(distcolname)) {"first("+c+") as axsdist"} else { "first("+c+") as "+c }).mkString(", ")
      //df1.sparkSession.sql("select "+firstSels+" from join2 group by zone, ra, dec having min("+distcolname+")")
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
