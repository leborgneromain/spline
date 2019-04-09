package za.co.absa.spline.core.listener

import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.util.QueryExecutionListener
import za.co.absa.spline.core.SparkLineageInitializer

class SplineQueryExecutionAdapter(sparkConf: SparkConf) extends QueryExecutionListener {

  private val listener = SparkLineageInitializer.constructBatchListener(sparkConf)

  override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit =
    listener.onSuccess(funcName, qe, durationNs)

  override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit =
    listener.onFailure(funcName, qe, exception)
}
