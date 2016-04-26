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

package com.sourcelearning.le03

import java.util.Properties
import scala.collection.Map
import scala.collection.mutable
import org.apache.spark.{Logging, TaskEndReason}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.executor.TaskMetrics
import org.apache.spark.scheduler.cluster.ExecutorInfo
import org.apache.spark.storage.BlockManagerId
import org.apache.spark.util.{Distribution, Utils}
import scala.collection.mutable.HashMap


private class BasicJobCounter extends SparkListener {
  var count = 0
  override def onJobEnd(job: SparkListenerJobEnd): Unit = {
    println(count)
    count += 1
  }
}

@DeveloperApi
sealed trait JobResult

@DeveloperApi
case object JobSucceeded extends JobResult

@DeveloperApi
class TaskInfo(
    val taskId: Long,
    val index: Int,
    val attempt: Int,
    val launchTime: Long,
    val executorId: String,
    val host: String,
    val speculative: Boolean) {

}




@DeveloperApi
class RDDInfo(
    val id: Int,
    val name: String,
    val numPartitions: Int,
    val parentIds: Seq[Int])
  extends Ordered[RDDInfo] {

  var numCachedPartitions = 0
  var memSize = 0L
  var diskSize = 0L
  var externalBlockStoreSize = 0L

  def isCached: Boolean =
    (memSize + diskSize + externalBlockStoreSize > 0) && numCachedPartitions > 0

  override def compare(that: RDDInfo): Int = {
    this.id - that.id
  }
}

@DeveloperApi
class StageInfo(
    val stageId: Int,
    val attemptId: Int,
    val name: String,
    val numTasks: Int,
    val rddInfos: Seq[RDDInfo],
    val parentIds: Seq[Int],
    val details: String) {
  /** When this stage was submitted from the DAGScheduler to a TaskScheduler. */
  var submissionTime: Option[Long] = None
  /** Time when all tasks in the stage completed or when the stage was cancelled. */
  var completionTime: Option[Long] = None
  /** If the stage failed, the reason why. */
  var failureReason: Option[String] = None
  /** Terminal values of accumulables updated during this stage. */

  def stageFailed(reason: String) {
    failureReason = Some(reason)
    completionTime = Some(System.currentTimeMillis)
  }

  private[le03] def getStatusString: String = {
    if (completionTime.isDefined) {
      if (failureReason.isDefined) {
        "failed"
      } else {
        "succeeded"
      }
    } else {
      "running"
    }
  }
}

@DeveloperApi
sealed trait SparkListenerEvent

@DeveloperApi
case class SparkListenerStageSubmitted(stageInfo: StageInfo, properties: Properties = null)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerStageCompleted(stageInfo: StageInfo) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerTaskStart(stageId: Int, stageAttemptId: Int, taskInfo: TaskInfo)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerTaskGettingResult(taskInfo: TaskInfo) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerTaskEnd(
    stageId: Int,
    stageAttemptId: Int,
    taskType: String,
    reason: TaskEndReason,
    taskInfo: TaskInfo,
    taskMetrics: TaskMetrics)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerJobStart(
    jobId: Int,
    time: Long,
    stageInfos: Seq[StageInfo],
    properties: Properties = null)
  extends SparkListenerEvent {
  // Note: this is here for backwards-compatibility with older versions of this event which
  // only stored stageIds and not StageInfos:
  val stageIds: Seq[Int] = stageInfos.map(_.stageId)
}

@DeveloperApi
case class SparkListenerJobEnd(
    jobId: Int,
    time: Long)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerEnvironmentUpdate(environmentDetails: Map[String, Seq[(String, String)]])
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerBlockManagerAdded(time: Long, blockManagerId: BlockManagerId, maxMem: Long)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerBlockManagerRemoved(time: Long, blockManagerId: BlockManagerId)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerUnpersistRDD(rddId: Int) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerExecutorAdded(time: Long, executorId: String, executorInfo: ExecutorInfo)
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerExecutorRemoved(time: Long, executorId: String, reason: String)
  extends SparkListenerEvent

/**
 * Periodic updates from executors.
 * @param execId executor id
 * @param taskMetrics sequence of (task id, stage id, stage attempt, metrics)
 */
@DeveloperApi
case class SparkListenerExecutorMetricsUpdate(
    execId: String,
    taskMetrics: Seq[(Long, Int, Int, TaskMetrics)])
  extends SparkListenerEvent

@DeveloperApi
case class SparkListenerApplicationStart(appName: String, appId: Option[String],
   time: Long, sparkUser: String, appAttemptId: Option[String]) extends SparkListenerEvent

@DeveloperApi
case class SparkListenerApplicationEnd(time: Long) extends SparkListenerEvent

/**
 * An internal class that describes the metadata of an event log.
 * This event is not meant to be posted to listeners downstream.
 */
private[le03] case class SparkListenerLogStart(sparkVersion: String) extends SparkListenerEvent

/**
 * :: DeveloperApi ::
 * Interface for listening to events from the Spark scheduler. Note that this is an internal
 * interface which might change in different Spark releases. Java clients should extend
 * {@link JavaSparkListener}
 */
@DeveloperApi
trait SparkListener {
  /**
   * Called when a stage completes successfully or fails, with information on the completed stage.
   */
  def onStageCompleted(stageCompleted: SparkListenerStageCompleted) { }

  /**
   * Called when a stage is submitted
   */
  def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) { }

  /**
   * Called when a task starts
   */
  def onTaskStart(taskStart: SparkListenerTaskStart) { }

  /**
   * Called when a task begins remotely fetching its result (will not be called for tasks that do
   * not need to fetch the result remotely).
   */
  def onTaskGettingResult(taskGettingResult: SparkListenerTaskGettingResult) { }

  /**
   * Called when a task ends
   */
  def onTaskEnd(taskEnd: SparkListenerTaskEnd) { }

  /**
   * Called when a job starts
   */
  def onJobStart(jobStart: SparkListenerJobStart) { }

  /**
   * Called when a job ends
   */
  def onJobEnd(jobEnd: SparkListenerJobEnd) { }

  /**
   * Called when environment properties have been updated
   */
  def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate) { }

  /**
   * Called when a new block manager has joined
   */
  def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded) { }

  /**
   * Called when an existing block manager has been removed
   */
  def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved) { }

  /**
   * Called when an RDD is manually unpersisted by the application
   */
  def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD) { }

  /**
   * Called when the application starts
   */
  def onApplicationStart(applicationStart: SparkListenerApplicationStart) { }

  /**
   * Called when the application ends
   */
  def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd) { }

  /**
   * Called when the driver receives task metrics from an executor in a heartbeat.
   */
  def onExecutorMetricsUpdate(executorMetricsUpdate: SparkListenerExecutorMetricsUpdate) { }

  /**
   * Called when the driver registers a new executor.
   */
  def onExecutorAdded(executorAdded: SparkListenerExecutorAdded) { }

  /**
   * Called when the driver removes an executor.
   */
  def onExecutorRemoved(executorRemoved: SparkListenerExecutorRemoved) { }
}

/**
 * :: DeveloperApi ::
 * Simple SparkListener that logs a few summary statistics when each stage completes
 */


private case class RuntimePercentage(executorPct: Double, fetchPct: Option[Double], other: Double)

private object RuntimePercentage {
  def apply(totalTime: Long, metrics: TaskMetrics): RuntimePercentage = {
    val denom = totalTime.toDouble
    val fetchTime = metrics.shuffleReadMetrics.map(_.fetchWaitTime)
    val fetch = fetchTime.map(_ / denom)
    val exec = (metrics.executorRunTime - fetchTime.getOrElse(0L)) / denom
    val other = 1.0 - (exec + fetch.getOrElse(0d))
    RuntimePercentage(exec, fetch, other)
  }
}
