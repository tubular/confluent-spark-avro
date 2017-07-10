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

/*
 * We have to use "com.databricks.spark.avro" because "SchemaConverters.toSqlType"
 * is only visible in the scope of the package.
 */
package com.databricks.spark.avro

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient
import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.Schema
import org.apache.commons.logging.LogFactory
import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.DataType

import scala.collection.JavaConverters._
import scalaz.Memo

/**
  * The class is used to generate deserialization UDFs for Avro messages in Kafka powered by Schema Registry.
  *
  * Usages:
  *   val utils = new ConfluentSparkAvroUtils("http://schema-registry.my.company.com:8081")
  *   val sparkUDF = utils.deserializerForSubject("my-awesome-subject-in-schema-registry")
  *
  * Notes:
  *   All attributes that have to be created from scratch on the executor side are marked with "@transient lazy".
  */
class ConfluentSparkAvroUtils(schemaRegistryURLs: String) extends Serializable {

  @transient private lazy val logger = LogFactory.getLog(this.getClass)

  @transient private lazy val schemaRegistry = {
    // Multiple URLs for HA mode.
    val urls = schemaRegistryURLs.split(",").toList.asJava
    // Store up to 128 schemas.
    val cacheCapacity = 128

    logger.info("Connecting to schema registry server at " + schemaRegistryURLs)

    new CachedSchemaRegistryClient(urls, cacheCapacity)
  }

  @transient private lazy val deserializer = new KafkaAvroDeserializer(schemaRegistry)

  def deserializerForSubject(subject: String, version: String): UserDefinedFunction = {
    udf(
      (payload: Array[Byte]) => {
        val avroSchema = avroSchemaForSubject(subject, version)
        val dataType = dataTypeForSubject(subject, version)
        val obj = deserializer.deserialize("", payload, avroSchema)
        val toSqlRow = sqlConverter(avroSchema, dataType)

        toSqlRow(obj).asInstanceOf[GenericRow]
      },
      dataTypeForSubject(subject, version)
    )
  }

  def deserializerForSubject(subject: String): UserDefinedFunction = deserializerForSubject(subject, "latest")

  @transient private lazy val avroSchemaForSubject: ((String, String)) => Schema = Memo.mutableHashMapMemo {
    subjectAndVersion =>
      var schemaId: Int = -1

      if (subjectAndVersion._2 == "latest") {
        schemaId = schemaRegistry.getLatestSchemaMetadata(subjectAndVersion._1).getId
      } else {
        schemaId = schemaRegistry.getSchemaMetadata(subjectAndVersion._1, subjectAndVersion._2.toInt).getId
      }

      logger.info("Resolving avro schema for subject " + subjectAndVersion._1 + " with version " + subjectAndVersion._2)

      schemaRegistry.getByID(schemaId)
  }

  @transient private lazy val dataTypeForSubject: ((String, String)) => DataType = Memo.mutableHashMapMemo {
    subjectAndVersion =>
      SchemaConverters.toSqlType(avroSchemaForSubject(subjectAndVersion._1, subjectAndVersion._2)).dataType
  }

  @transient private lazy val sqlConverter: ((Schema, DataType)) => Function[AnyRef, AnyRef] = Memo.mutableHashMapMemo {
    schemaAndType =>
      SchemaConverters.createConverterToSQL(schemaAndType._1, schemaAndType._2)
  }
}
