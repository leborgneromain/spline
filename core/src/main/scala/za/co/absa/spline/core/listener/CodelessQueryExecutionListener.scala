/*
 * Copyright 2019 ABSA Group Limited
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

package za.co.absa.spline.core.listener

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import za.co.absa.spline.core.SparkLineageInitializer.createQueryExecutionListener

// FIXME explain the lazy approach and double prevention
class CodelessQueryExecutionListener() extends QueryExecutionListener {

  private lazy val listener = constructBatchListener()

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
    listener.onSuccess(funcName, qe, durationNs)

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
    listener.onFailure(funcName, qe, exception)

  def constructBatchListener(): QueryExecutionListener = {
    val sparkSession = SparkSession.getActiveSession.getOrElse(
      SparkSession.getDefaultSession.getOrElse(throw new IllegalStateException("Session is unexpectedly missing.")))
    createQueryExecutionListener(sparkSession)
  }
}
