/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.json

import java.io.{Closeable, InputStream}

import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.Text

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.catalyst.util.BadRecordException
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils
import sun.misc.Unsafe

class FPGAJsonParser(
    schema: StructType,
    val options: JSONOptions) extends Logging {

  private def initJniParser: JniParser = {
    val jp = new JniParser
    jp.setSchema(jniFields, jniTypeArray)
    jp
  }

  private def initJniParser2: FpgaJsonParserImpl = {
    val jp2 = new FpgaJsonParserImpl
    jp2.setSchema(jniFields, jniTypeArray)
    jp2
  }

  val convertedRow = new UnsafeRow(schema.length)

  private def jniConverter(buf: Array[Byte], offset: Int, length: Int): UnsafeRow = {
    convertedRow.pointTo(buf, Unsafe.ARRAY_BYTE_BASE_OFFSET + offset, length)
    convertedRow
  }

  private def jniConverter2(addr: Long, length: Int): UnsafeRow = {
    convertedRow.pointTo(null, addr, length)
    convertedRow
  }

  private val jniFields = schema.map(field => field.name).mkString(",")
  private val jniTypeArray =
    schema.map(field => FPGAJsonParser.typeConverter(field.dataType)).toArray

  // only support maximum 4 fields( double/String). String with fixed length 128 bytes
  // DDDD 40 bytes
  // DDDS 168 bytes
  // DDSS 296 bytes
  // DSSS 424 bytes
  // SSSS 552 bytes
  private val stringFieldCount = jniTypeArray.count((e: Int) => e match {
    case 7 => true
    case _ => false
  })

  private val constRowSize = stringFieldCount * (128 + 8) + 8*(4-stringFieldCount) + 8

  //println(s"const row size: $constRowSize")

  def parseText(
      record: Text,
      recordLiteral: Text => UTF8String): Seq[InternalRow] = {
    Utils.tryWithResource(initJniParser) { parser =>
      parser.parseJson(FPGAJsonParser.textToString(record)) match {
        case null => Nil
        case byteArray => {
          val rowSize = Platform.getInt(byteArray, Unsafe.ARRAY_BYTE_BASE_OFFSET)
          Seq(jniConverter(byteArray, 4, rowSize))
        }
      }
    }
    // should throw [[[BadRecordException]]] in parser if failed
  }

  def parseInputStream(
      record: InputStream,
      recordLiteral: InputStream => UTF8String): Seq[InternalRow] = {

    Utils.tryWithResource(initJniParser) { parser =>
      parser.parseJson(FPGAJsonParser.inputStreamToString(record)) match {
        case null => Nil
        case byteArray => {
          val rowSize = Platform.getInt(byteArray, Unsafe.ARRAY_BYTE_BASE_OFFSET)
          Seq(jniConverter(byteArray, 4, rowSize))
        }
      }
    }
    // should throw BadRecordException in parser if failed
  }

  def parseFile(
      records: Iterator[Text],
      recordLiteral: Text => UTF8String): Iterator[InternalRow] = {
    Utils.tryWithResource(initJniParser2) { parser =>
      FPGAJsonParser.recordsToChunks(records).map(parser.parseJson).flatMap {
        case null => Nil.toIterator
        case byteArray => new Iterator[InternalRow] {
          private var currentPos = 0
          override def hasNext: Boolean = currentPos < byteArray.length
          override def next(): InternalRow = {
            // val rowSize = Platform.getInt(byteArray, Unsafe.ARRAY_BYTE_BASE_OFFSET + currentPos)
            // currentPos += 4
            val rowSize = constRowSize
            val rowOffset = currentPos
            currentPos += rowSize
            // TODO
            // scalastyle:off
            //println("------decoded bytes---------")
            //byteArray.foreach(b => print(b.toInt + ","))
            //println("-------------------")
            // scalastyle:on
            jniConverter(byteArray, rowOffset, rowSize)
          }
        }
      }
    }
    // should throw [[[BadRecordException]]] in parser if failed
  }
  def arrayToTuple(arrs : Array[Long]) : (Long, Long) = {
    arrs match {
      case Array(buff_addr, buff_size) => {
        //println(s"addr:$buff_addr, size: $buff_size")
        (buff_addr, buff_size)
      }
      case _ => {
        //println("wrong return value from FPGAJsonParser's native method")
        (-1, 0)
      }
    }
  }
  def parseFileWithBufferAddr(
                 records: Iterator[Text],
                 recordLiteral: Text => UTF8String): Iterator[InternalRow] = {
    Utils.tryWithResource(initJniParser2) { parser =>
      FPGAJsonParser.recordsToChunks(records).map((x : String) =>
        arrayToTuple(parser.parseJson2(x))).flatMap {
        case (-1, _) => Nil.toIterator
        case (0, _) => Nil.toIterator
        case (_, 0) => {
          println("bufer size is 0! unknown error!")
          Nil.toIterator
        }
        case (buff_addr, buff_size) => new Iterator[InternalRow] {
          private var cnt = 0
          private val max = buff_size
          override def hasNext: Boolean = {
            cnt*constRowSize < max
          }
          override def next(): InternalRow = {
            val rowSize = constRowSize
            val rowOffset = buff_addr + constRowSize * cnt
            cnt += 1
            // TODO
            // scalastyle:off
            //println(s"------count:$cnt, decoded $constRowSize bytes from address $rowOffset (max:$max) ---------")
            //val p = rowOffset;
            //for (p <- rowOffset to rowOffset +rowSize) {
            //  print(Platform.getByte(null, p) + ",")
            //}
            //println("-------------------")
            // scalastyle:on
            jniConverter2(rowOffset, rowSize)
          }
        }
      }
    }
    // should throw [[[BadRecordException]]] in parser if failed
  }
}

private object FPGAJsonParser {
  System.loadLibrary("FpgaJsonParserImpl")

  private val boolean = 1
  private val short = 2
  private val int = 3
  private val long = 4
  private val float = 5
  private val double = 6
  private val string = 7

  private val MAX_LINES = 1024 * 128
  def typeConverter(dt: DataType): Int = dt match {
    case BooleanType => boolean
    case ShortType => short
    case IntegerType => int
    case LongType => long
    case FloatType => float
    case DoubleType => double
    case StringType => string
    case t => throw new NotImplementedError(s"Not support ${t.typeName} in jni parser")
  }
  def inputStreamToString(is: InputStream): String = {
    IOUtils.toString(is, "UTF-8")
  }
  def textToString(text: Text): String = text.toString
  def recordsToChunks(records: Iterator[Text]): Iterator[String] = {
    new Iterator[String] {
      override def hasNext: Boolean = records.hasNext
      override def next(): String = {
        records.take(MAX_LINES).map(FPGAJsonParser.textToString).mkString
      }
    }
  }
}

class JniParser extends Closeable {
  // -- Native methods
  @native def setSchema(schemaFieldNames: String, schemaFieldTypes: Array[Int]): Boolean
  @native def parseJson(s: String): Array[Byte]
  @native def close(): Unit
}

class FpgaJsonParserImpl extends Closeable {
  // -- Native methods
  @native def setSchema(schemaFieldNames: String, schemaFieldTypes: Array[Int]): Boolean
  @native def parseJson(s: String): Array[Byte]
  // should have only two elements. The first long is buffer address, second one is buffer size
  @native def parseJson2(s: String): Array[Long]
  @native def close(): Unit
}
