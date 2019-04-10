/*
 * Copyright 2017 ABSA Group Limited
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

package za.co.absa.spline.core

import org.apache.commons.configuration._
import org.apache.hadoop.{conf => hadoop}
import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.QueryExecutionListener
import org.slf4s.Logging
import za.co.absa.spline.core.conf.SplineConfigurer.SplineMode._
import za.co.absa.spline.core.conf._
import za.co.absa.spline.coresparkadapterapi.SparkVersionRequirement

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext
import scala.util.Try
import scala.util.control.NonFatal

/**
  * The object contains logic needed for initialization of the library
  */
object SparkLineageInitializer extends Logging {

  def enableLineageTracking(sparkSession: SparkSession): SparkSession = {
    SparkSessionWrapper(sparkSession).enableLineageTracking()
  }

  def enableLineageTracking(sparkSession: SparkSession, configurer: SplineConfigurer): SparkSession = {
    SparkSessionWrapper(sparkSession).enableLineageTracking(configurer)
  }

  def constructBatchListener(sparkConf: SparkConf, initialized: ThreadLocal[Boolean]): QueryExecutionListener = {
    val configurer = new DefaultSplineConfigurer(defaultSplineConfiguration(sparkConf), getHadoopConf(sparkConf))
    if (configurer.splineMode != DISABLED) {
        modeAwareListenerInit(configurer, threadLocalGetOrSetIsInitiliazed(initialized))
          .getOrElse(throw registrationPreventingException())
    } else {
      throw registrationPreventingException()
    }
  }

  private def registrationPreventingException() = new UnsupportedOperationException(
          "This exception signals to Spark that Spline listener shouldn't be registered.")

  private def modeAwareListenerInit(configurer: SplineConfigurer, getOrSetIsInitialized: () => Boolean): Option[QueryExecutionListener] = {
    if (configurer.splineMode != DISABLED) {
      if (!getOrSetIsInitialized()) {
        log info s"Spline v${SplineBuildInfo.version} is initializing..."
        try {
          val listener = attemptInitialization(configurer)
          log info s"Spline successfully initialized. Spark Lineage tracking is ENABLED."
          Some(listener)
        } catch {
          case NonFatal(e) if configurer.splineMode == BEST_EFFORT =>
            log.error(s"Spline initialization failed! Spark Lineage tracking is DISABLED.", e)
            None
        }
      } else {
        log.warn("Spline lineage tracking is already initialized!")
        None
      }
    } else {
      None
    }
  }

  private def threadLocalGetOrSetIsInitiliazed(initialized: ThreadLocal[Boolean])(): Boolean = {
    if (!initialized.get()) {
      initialized.set(true)
      false
    } else {
      true
    }
  }

  /**
    * The method tries to initialize the library with external settings.
    *
    * @param configurer External settings
    */
  private def attemptInitialization(configurer: SplineConfigurer): QueryExecutionListener = {
    SparkVersionRequirement.instance.requireSupportedVersion()
    // TODO configurer.streamingQueryListener
    configurer.queryExecutionListener
  }

  private[core] def defaultSplineConfiguration(sparkConf: SparkConf) = {
    val splinePropertiesFileName = "spline.properties"

    val systemConfOpt = Some(new SystemConfiguration)
    val propFileConfOpt = Try(new PropertiesConfiguration(splinePropertiesFileName)).toOption
    val hadoopConfOpt = Some(new HadoopConfiguration(SparkHadoopUtil.get.newConfiguration(sparkConf)))
    val sparkConfOpt = Some(new SparkConfiguration(sparkConf))

    new CompositeConfiguration(Seq(
      hadoopConfOpt,
      sparkConfOpt,
      systemConfOpt,
      propFileConfOpt
    ).flatten.asJava)
  }

  private def getHadoopConf(sparkConf: SparkConf): hadoop.Configuration = {
    SparkHadoopUtil.get.newConfiguration(sparkConf)
  }

  /**
    * The class is a wrapper around Spark session and performs all necessary registrations and procedures for initialization of the library.
    *
    * @param sparkSession A Spark session
    */
  implicit class SparkSessionWrapper(sparkSession: SparkSession) {

    private implicit val executionContext: ExecutionContext = ExecutionContext.global
    private def defaultSplineConfigurer = new DefaultSplineConfigurer(
        defaultSplineConfiguration(),
        sparkSession.sparkContext.hadoopConfiguration)

    /**
      * The method performs all necessary registrations and procedures for initialization of the library.
      *
      * @param configurer A collection of settings for the library initialization
      * @return An original Spark session
      */
    def enableLineageTracking(configurer: SplineConfigurer = defaultSplineConfigurer): SparkSession = {
      val splineConfiguredForCodelessInit = sparkSession.sparkContext.getConf
        .getOption(org.apache.spark.sql.internal.StaticSQLConf.QUERY_EXECUTION_LISTENERS.key).isDefined
      if (!splineConfiguredForCodelessInit) {
        sparkSession.synchronized {
          // FIXME check if was not inited via codeless as well!!!
          SparkLineageInitializer.modeAwareListenerInit(configurer, getOrSetIsInitialized)
            .foreach(sparkSession.listenerManager.register(_))
          //         TODO: SL-128
          //        sparkSession.streams addListener configurer.streamingQueryListener
        }
      } else {
        log.warn("""
          Spline lineage tracking is also configured for codeless initialization.
          It won't be initialized by this code call to enableLineageTracking now."""")
      }
      sparkSession
    }

    def defaultSplineConfiguration(): CompositeConfiguration =
      SparkLineageInitializer.defaultSplineConfiguration(sparkSession.sparkContext.getConf)

    private def getOrSetIsInitialized(): Boolean = {
      val sessionConf = sparkSession.conf
      sessionConf getOption initFlagKey match {
        case Some(_) =>
          true
        case None =>
          sessionConf.set(initFlagKey, true.toString)
          false
      }
    }
  }

  val initFlagKey = "spline.initialized_flag"
}
