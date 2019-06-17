
package org.dirac.axs.UDFs

import org.apache.spark.sql.api.java.{UDF2, UDF3}

class array_filtered_count extends UDF2[Seq[Integer], Integer, Integer]  {

  override def call(ids: Seq[Integer], selected_id: Integer): Integer = {
    val selected = ids filter { _ == selected_id }
    selected.size
  }

}


class array_filtered_mean extends UDF3[Seq[Float], Seq[Integer], Integer, Double]  {

  override def call(values: Seq[Float], ids: Seq[Integer], selected_id: Integer): Double = {
    val selected = (values zip ids) filter { _._2 == selected_id } map { _._1 }
    val mean = selected.sum / selected.size.toFloat
    mean
  }

}

class array_filtered_stddev extends UDF3[Seq[Float], Seq[Integer], Integer, Double]  {

  override def call(values: Seq[Float], ids: Seq[Integer], selected_id: Integer): Double = {
    val selected = (values zip ids) filter { _._2 == selected_id } map { _._1 }
    val mean = selected.sum / selected.size.toFloat
    val mean_squared = selected.map(x => math.pow(x - mean, 2)).sum / selected.size.toFloat
    math.sqrt(mean_squared)
  }

}
