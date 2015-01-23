/*
 * Copyright 2015 Databricks
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.databricks.spark.avro

import java.sql.Timestamp
import java.util.HashMap

import scala.collection.immutable.Map

import org.apache.spark.sql._
import org.apache.spark.SparkContext._

import org.apache.avro.generic.GenericData.Record
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred._
import org.apache.avro.SchemaBuilder.{RecordBuilder, FieldAssembler}
import org.apache.avro.{SchemaBuilder, Schema}

import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.JobConf


/** 
 * This object provides a save() method that is used to save SchemaRDD as avro file.
 * To do this, we first convert the schema and then convert each row of the RDD to corresponding
 * avro types. One remark worth mentioning is the structName parameter that functions have. Avro
 * records have a name associated with them, which must be unique. Since SturctType in sparkSQL
 * doesn't have a name associated with it, we are taking the name of the last structure field that
 * the current structure is a child of. For example if the row at the top level had a field called
 * "X", which happens to be a structure, we would call that structure "X". When we process original
 * rows, they get a name "topLevelRecord".
 */
object AvroSaver {

  private def convertTypeToAvro[T](dataType: DataType, 
      schemaBuilder: SchemaBuilder.BaseTypeBuilder[T],
      structName: String): T = {
    dataType match {
      case ByteType => schemaBuilder.bytesType()
      case ShortType => schemaBuilder.intType()
      case IntegerType => schemaBuilder.intType()
      case LongType => schemaBuilder.longType()
      case FloatType => schemaBuilder.floatType()
      case DoubleType => schemaBuilder.doubleType()
      case DecimalType() => schemaBuilder.stringType()
      case StringType => schemaBuilder.stringType()
      case BinaryType => schemaBuilder.bytesType()
      case BooleanType => schemaBuilder.booleanType()
      case TimestampType => schemaBuilder.longType()

      case ArrayType(_, _) =>
        val builder = SchemaBuilder.builder()
        val elementSchema = convertTypeToAvro(dataType.asInstanceOf[ArrayType].elementType,
          builder, structName)

        schemaBuilder.array().items(elementSchema)

      case MapType(StringType, _, _) =>
        val builder = SchemaBuilder.builder()
        val valueSchema = convertTypeToAvro(dataType.asInstanceOf[MapType].valueType,
          builder, structName)

        schemaBuilder.map().values(valueSchema)

      case StructType(_) =>
        convertStructToAvro(dataType.asInstanceOf[StructType],
          schemaBuilder.record(structName))

      case other => sys.error(s"Unexpected type $dataType")
    }
  }

  private def convertFieldTypeToAvro[T](dataType: DataType,
      newFieldBuilder: SchemaBuilder.FieldTypeBuilder[T],
      structName: String): SchemaBuilder.FieldDefault[_, _] = {
    dataType match {
      case ByteType => newFieldBuilder.bytesType()
      case ShortType => newFieldBuilder.intType()
      case IntegerType => newFieldBuilder.intType()
      case LongType => newFieldBuilder.longType()
      case FloatType => newFieldBuilder.floatType()
      case DoubleType => newFieldBuilder.doubleType()
      case DecimalType() => newFieldBuilder.stringType()
      case StringType => newFieldBuilder.stringType()
      case BinaryType => newFieldBuilder.bytesType()
      case BooleanType => newFieldBuilder.booleanType()
      case TimestampType => newFieldBuilder.longType()

      case ArrayType(_, _) =>
        val builder = SchemaBuilder.builder()
        val elementSchema = convertTypeToAvro(dataType.asInstanceOf[ArrayType].elementType,
          builder, structName)

        newFieldBuilder.array().items(elementSchema)

      case MapType(StringType, _, _) =>
        val builder = SchemaBuilder.builder()
        val valueSchema = convertTypeToAvro(dataType.asInstanceOf[MapType].valueType,
          builder, structName)

        newFieldBuilder.map().values(valueSchema)

      case StructType(_) =>
        convertStructToAvro(dataType.asInstanceOf[StructType],
          newFieldBuilder.record(structName))

      case other => sys.error(s"Unexpected type $dataType")
    }
  }

  private def convertStructToAvro[T](structType: StructType, schemaBuilder: RecordBuilder[T]): T = {
    val fieldsAssembler: FieldAssembler[T] = schemaBuilder.fields()
    structType.fields foreach { field =>
      val newField = fieldsAssembler.name(field.name).`type`()

      if (field.nullable) {
        convertTypeToAvro(field.dataType, newField.optional(), field.name)
      } else {
        convertFieldTypeToAvro(field.dataType, newField, field.name).noDefault()
      }
    }
    fieldsAssembler.endRecord()
  }

  private def convertValueToAvro(item: Any, dataType: DataType, structName: String): Any = {
    if (item == null) {
      // Since some schema fields might be nullable, item can be null. If so, just return it
      // without further processing
      return null
    }

    dataType match {
      case ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType | StringType |
           BinaryType | BooleanType =>
        item

      case DecimalType() =>
        item.toString

      case TimestampType =>
        item.asInstanceOf[Timestamp].getTime

      case ArrayType(_, _) =>
        val sourceArray = item.asInstanceOf[Seq[Any]]
        val targetArray = new Array[Any](sourceArray.size)
        var idx = 0

        while (idx < sourceArray.size) {
          targetArray(idx) = convertValueToAvro(sourceArray(idx),
            dataType.asInstanceOf[ArrayType].elementType, structName)
          idx += 1
        }

        targetArray

      case MapType(StringType, _, _) =>
        val javaMap = new HashMap[String, Any]()
        item.asInstanceOf[Map[String, Any]].foreach(kv => javaMap.put(kv._1,
          convertValueToAvro(kv._2, dataType.asInstanceOf[MapType].valueType, structName)))
        javaMap

      case StructType(_) => rowToAvro(item.asInstanceOf[Row], dataType.asInstanceOf[StructType],
        structName)._1.datum()
    }
  }

  private def rowToAvro(row: Row, structType: StructType,
      structName: String):(AvroKey[GenericRecord], NullWritable) = {
    val builder = SchemaBuilder.record(structName)
    val schema: Schema = convertStructToAvro(structType, builder)
    val record = new Record(schema)

    val fieldIterator = structType.fields.iterator
    val rowIterator = row.iterator

    while (fieldIterator.hasNext) {
      val field = fieldIterator.next
      record.put(field.name, convertValueToAvro(rowIterator.next, field.dataType, field.name))
    }

    (new AvroKey(record), NullWritable.get())
  }

  def save(schemaRDD: SchemaRDD, location: String): Unit = {
    val jobConf = new JobConf(schemaRDD.sparkContext.hadoopConfiguration)
    val builder = SchemaBuilder.record("topLevelRecord")
    val schema = convertStructToAvro(schemaRDD.schema, builder)
    AvroJob.setOutputSchema(jobConf, schema)

    schemaRDD.map(rowToAvro(_, schemaRDD.schema, "topLevelRecord")).saveAsHadoopFile(location,
      classOf[AvroWrapper[GenericRecord]],
      classOf[NullWritable],
      classOf[AvroOutputFormat[GenericRecord]],
      jobConf)
  }
}