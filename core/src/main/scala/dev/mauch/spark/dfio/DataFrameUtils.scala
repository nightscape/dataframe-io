package dev.mauch.spark.dfio

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, to_json}
import org.apache.spark.sql.types.{ArrayType, StructType}

object DataFrameUtils {
  def flattenSchema(schema: StructType): Array[Column] = schema.fields.map { f =>
    f.dataType match {
      case st: StructType => to_json(col(f.name)).as(f.name)
      case st: ArrayType => to_json(col(f.name)).as(f.name)
      case _ => col(f.name)
    }
  }
}
