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

import java.io.InputStream

import com.google.common.io.ByteStreams
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Text

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.catalyst.json.JSONOptions
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

abstract class JsonFPGADataSource {
  val isSplitable: Boolean
  def readFile(
      conf: Configuration,
      file: PartitionedFile,
      parser: FPGAJsonParser,
      schema: StructType): Iterator[InternalRow]

  /**
   * Supports multi records in one feed.
   */
  def readFile2(
      conf: Configuration,
      file: PartitionedFile,
      parser: FPGAJsonParser,
      schema: StructType): Iterator[InternalRow]
}

object JsonFPGADataSource {
  def apply(options: JSONOptions): JsonFPGADataSource = {
    if (options.multiLine) {
      MultiLineJsonFPGADataSource
    } else {
      TextInputJsonFPGADataSource
    }
  }
}

object TextInputJsonFPGADataSource extends JsonFPGADataSource {
  override val isSplitable: Boolean = {
    // splittable if the underlying source is
    true
  }

  override def readFile(
      conf: Configuration,
      file: PartitionedFile,
      parser: FPGAJsonParser,
      schema: StructType): Iterator[InternalRow] = {
    val linesReader = new HadoopFileLinesReader(file, conf)
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => linesReader.close()))
    val safeParser = new FailureSafeParser[Text](
      input => parser.parseText(input, textToUTF8String),
      parser.options.parseMode,
      schema,
      parser.options.columnNameOfCorruptRecord)
    linesReader.flatMap(safeParser.parse)
  }
  override def readFile2(
      conf: Configuration,
      file: PartitionedFile,
      parser: FPGAJsonParser,
      schema: StructType): Iterator[InternalRow] = {
    val linesReader = new HadoopFileLinesReader(file, conf)
    Option(TaskContext.get()).foreach(_.addTaskCompletionListener(_ => linesReader.close()))
    // TODO failure handle
    parser.parseFileWithFileNameAndGetAddress(file.filePath)
  }

  private def textToUTF8String(value: Text): UTF8String = {
    UTF8String.fromBytes(value.getBytes, 0, value.getLength)
  }
}

object MultiLineJsonFPGADataSource extends JsonFPGADataSource {
  override val isSplitable: Boolean = {
    false
  }

  override def readFile(
      conf: Configuration,
      file: PartitionedFile,
      parser: FPGAJsonParser,
      schema: StructType): Iterator[InternalRow] = {
    def partitionedFileString(ignored: Any): UTF8String = {
      Utils.tryWithResource {
        CodecStreams.createInputStreamWithCloseResource(conf, file.filePath)
      } { inputStream =>
        UTF8String.fromBytes(ByteStreams.toByteArray(inputStream))
      }
    }

    val safeParser = new FailureSafeParser[InputStream](
      input => parser.parseInputStream(input, partitionedFileString),
      parser.options.parseMode,
      schema,
      parser.options.columnNameOfCorruptRecord)

    safeParser.parse(
      CodecStreams.createInputStreamWithCloseResource(conf, file.filePath))
  }
  override def readFile2(
      conf: Configuration,
      file: PartitionedFile,
      parser: FPGAJsonParser,
      schema: StructType): Iterator[InternalRow] = {
    // here each file is a json value, so identical
    readFile(conf, file, parser, schema)
  }
}