package com.github.potix2.spark.google.spreadsheets

import com.google.api.services.sheets.v4.model.{CellData, ExtendedValue, RowData}
import org.apache.spark.sql.{Row, RowFactory}
import org.apache.spark.sql.types.{DataTypes, StructType}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer

object Util {
  import Util._

  def convert(schema: StructType, row: Row): Map[String, Object] =
    schema.iterator.zipWithIndex.map { case (f, i) => f.name -> row(i).asInstanceOf[AnyRef]} toMap

  def toRowData(row: Row): RowData = {
    new RowData().setValues(
      row.schema.fields.zipWithIndex.map { case (f, i) =>
        new CellData()
          .setUserEnteredValue(
            f.dataType match {
              case DataTypes.StringType => new ExtendedValue().setStringValue(if(row.isNullAt(i)) null else row.getString(i))
              case DataTypes.LongType => new ExtendedValue().setNumberValue(if(row.isNullOrEmpty(i))  null else row.getLong(i).toDouble)
              case DataTypes.IntegerType => new ExtendedValue().setNumberValue(if(row.isNullOrEmpty(i)) null else row.getInt(i).toDouble)
              case DataTypes.FloatType => new ExtendedValue().setNumberValue(if(row.isNullOrEmpty(i)) null else row.getFloat(i).toDouble)
              case DataTypes.BooleanType => new ExtendedValue().setBoolValue(if(row.isNullAt(i)) null else row.getBoolean(i))
              case DataTypes.DateType => new ExtendedValue().setStringValue(if(row.isNullAt(i)) null else row.getDate(i).toString)
              case DataTypes.ShortType => new ExtendedValue().setNumberValue(if(row.isNullOrEmpty(i)) null else row.getShort(i).toDouble)
              case DataTypes.TimestampType => new ExtendedValue().setStringValue(if(row.isNullAt(i)) null else row.getTimestamp(i).toString)
              case DataTypes.DoubleType => new ExtendedValue().setNumberValue(if(row.isNullOrEmpty(i)) null else row.getDouble(i))
            }
          )
      }.toList.asJava
    )
  }
  implicit class RowExt (row: Row){
    def isNullOrEmpty(idx: Integer): Boolean =
      row.isNullAt(idx) || row.get(idx).toString.equals("")
  }

}
