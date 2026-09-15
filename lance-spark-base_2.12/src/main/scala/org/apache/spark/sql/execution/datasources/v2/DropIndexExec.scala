/*
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
package org.apache.spark.sql.execution.datasources.v2

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, GenericInternalRow}
import org.apache.spark.sql.catalyst.plans.logical.LanceDropIndexOutputType
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.unsafe.types.UTF8String
import org.lance.spark.LanceDataset
import org.lance.spark.utils.Utils

/**
 * Physical execution of DROP INDEX for Lance datasets.
 *
 * Removes the named index from the dataset manifest via lance-core's dropIndex API.
 * Physical index files are not deleted; they are cleaned up by VACUUM.
 */
case class LanceDropIndexExec(
    catalog: TableCatalog,
    ident: Identifier,
    indexName: String) extends LeafV2CommandExec {

  override def output: Seq[Attribute] = LanceDropIndexOutputType.SCHEMA

  override protected def run(): Seq[InternalRow] = {
    val lanceDataset = LanceDataset.requireWritable(catalog.loadTable(ident), "DropIndex")

    val readOptions = lanceDataset.readOptions()

    val dataset = Utils.openDatasetBuilder(readOptions).build()
    try {
      dataset.dropIndex(indexName)
    } catch {
      // lance-core reports a missing index as a bare RuntimeException whose message names the
      // index and its own Rust source path but not the table, so a session working over several
      // tables cannot tell which one was meant. Keep the cause and lead with both identifiers.
      case e: Exception =>
        throw new RuntimeException(
          s"DROP INDEX failed for index '$indexName' on table ${ident.toString}",
          e)
    } finally {
      dataset.close()
    }

    Seq(new GenericInternalRow(Array[Any](
      UTF8String.fromString(indexName),
      UTF8String.fromString("dropped"))))
  }
}
