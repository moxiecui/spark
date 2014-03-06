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

package org.apache.spark.ui

import java.util.Properties

import scala.collection.mutable

import org.apache.spark.{SparkConf, ExceptionFailure, Success, FetchFailed}
import org.apache.spark.executor.{ShuffleWriteMetrics, TaskMetrics}
import org.apache.spark.scheduler._
import org.apache.spark.storage._
import org.apache.spark.util.FileLogger

/** Simple standalone micro benchmark for event logging */
object UIMicrobenchmark {

  def main(args: Array[String]) {

    // Initialize logging environment
    val conf = new SparkConf
    conf.set("spark.eventLog.enabled", "true")
    val listenerBus = new SparkListenerBus
    val iterations = Seq[Int](
      1000, 5000, 10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000)
    val numEventsToDuration = mutable.Map[Int, Long]()

    // Start logging events
    iterations.foreach { numEvents =>
      println("* Logging %d events...".format(numEvents))
      val eventLogger = new EventLoggingListener("crown", conf)
      listenerBus.addListener(eventLogger)
      val startTime = System.currentTimeMillis()
      var i = 0
      while (i < numEvents) {
        listenerBus.postToListeners(makeRandomEvent(), Seq(eventLogger))
        i += 1
      }
      eventLogger.stop()
      val duration = System.currentTimeMillis() - startTime
      println("-> Took %d ms!\n".format(duration))
      numEventsToDuration(numEvents) = duration
    }

    // Output result
    val fileLogger = new FileLogger("/tmp/spark-events/result")
    iterations.foreach { numEvents =>
      val duration = numEventsToDuration(numEvents)
      println("Logging %d events took %d ms".format(numEvents, duration))
      fileLogger.logLine("%d %d".format(numEvents, duration))
    }
    fileLogger.close()
  }

  /**
   * Generate a random SparkListenerEvent with random parameters
   */
  def makeRandomEvent(): SparkListenerEvent = {
    val seed = (Math.random() * 10).toInt
    val a = (Math.random() * 10).toInt
    val b = (Math.random() * 100).toInt
    val c = (Math.random() * 100).toInt
    val d = (Math.random() * 100).toInt
    val e = (Math.random() * 100).toInt
    val aa = (Math.random() * 10000).toLong
    val bb = (Math.random() * 10000).toLong
    val cc = (Math.random() * 10000).toLong
    val dd = (Math.random() * 10000).toLong
    val time = System.currentTimeMillis()
    val time1 = System.currentTimeMillis()
    val time2 = System.currentTimeMillis()
    seed match {
      case 0 =>
        val stageInfo = makeStageInfo(a, b, cc, dd)
        SparkListenerStageSubmitted(stageInfo, properties)
      case 1 =>
        val stageInfo = makeStageInfo(c, e, cc, bb)
        SparkListenerStageCompleted(stageInfo)
      case 2 =>
        val taskInfo = makeTaskInfo(cc, c, dd)
        SparkListenerTaskStart(a, taskInfo)
      case 3 =>
        val taskInfo = makeTaskInfo(aa, e, time)
        SparkListenerTaskGettingResult(taskInfo)
      case 4 =>
        val taskInfo = makeTaskInfo(aa, b, cc)
        val taskMetrics = makeTaskMetrics(aa, bb, cc, dd)
        val taskEndReason = makeTaskEndReason(c, d, e, aa, time1)
        SparkListenerTaskEnd(c, "Apocalypse", taskEndReason, taskInfo, taskMetrics)
      case 5 =>
        val stageIds = Seq[Int](a, b, 3, c, d, e, a + b, c - d)
        SparkListenerJobStart(b, stageIds, properties)
      case 6 =>
        val jobResult = makeJobResult(c, d)
        SparkListenerJobEnd(e, jobResult)
      case 7 =>
        val details = Map[String, Seq[(String, String)]](
          "JVM Information" -> Seq(("yo", "lo"), ("be", "to")),
          "Spark Properties" -> Seq(("winner", "loser"), ("to", "ve")),
          "System Properties" -> Seq(("thinner", "fatter"), ("I", "say")),
          "Classpath Entries" -> Seq(("to", "the"), ("irresistible", "joy")))
        SparkListenerEnvironmentUpdate(details)
      case 8 =>
        val statusList = (1 to (time + time2).toInt % 3).map { i =>
          makeStorageStatus(cc, dd, time1, a, b)
        }
        SparkListenerExecutorsStateChange(statusList)
      case 9 =>
        SparkListenerUnpersistRDD(a - b)
    }
  }

  /** Miscellaneous types */

  val properties = {
    val p = new Properties()
    p.setProperty("Ukraine", "Kiev")
    p.setProperty("Russia", "Moscow")
    p.setProperty("France", "Paris")
    p.setProperty("Germany", "Berlin")
    p
  }

  def makeRddInfo(a: Int, b: Int, c: Long, d: Long) = {
    val r = new RDDInfo(a, "mayor", b, StorageLevel.MEMORY_AND_DISK)
    r.numCachedPartitions = a
    r.memSize = c
    r.diskSize = d
    r
  }

  def makeTaskInfo(a: Long, b: Int, c: Long) =
    new TaskInfo(a, b, c, "executor", "your kind sir", TaskLocality.NODE_LOCAL)

  def makeTaskMetrics(a: Long, b: Long, c: Long, d: Long) = {
    val t = new TaskMetrics
    val sw = new ShuffleWriteMetrics
    t.hostname = "Happy"
    t.executorDeserializeTime = a
    t.executorRunTime = b
    t.resultSize = c
    t.jvmGCTime = d
    t.resultSerializationTime = a - b
    t.memoryBytesSpilled = a + b
    t.shuffleReadMetrics = None
    sw.shuffleBytesWritten = c * d - a * b
    sw.shuffleWriteTime = c - d * a + b
    t.shuffleWriteMetrics = Some(new ShuffleWriteMetrics)
    t.updatedBlocks = Some((1 to (c % 5).toInt).map { i =>
      (makeBlockId(c.toInt % i, d.toInt % i), makeBlockStatus(a.toInt % i, b.toInt % i))
    }.toSeq)
    t
  }

  def makeStageInfo(a: Int, b: Int, c: Long, d: Long) = {
    val taskInfos = (1 to (b % 20)).map { i =>
      (makeTaskInfo(c - d % i, a - i, c + i % 3), makeTaskMetrics(c, i, c % 100, d % i))
    }.toBuffer
    new StageInfo(a, "greetings", b, makeRddInfo(a, b, c, d), taskInfos)
  }

  def makeStorageStatus(a: Long, b: Long, c: Long, d: Int, e: Int) = {
    val blocks = mutable.Map[BlockId, BlockStatus]()
    (1 to ((a - b).toInt % 10)).foreach { i =>
      blocks(makeBlockId(d - i, e + i)) = makeBlockStatus(a * i, b / i)
    }
    new StorageStatus(makeBlockManagerId(d, e), c - b, blocks)
  }

  def makeBlockId(a: Int, b: Int) = RDDBlockId(a, b)

  def makeBlockStatus(a: Long, b: Long) = BlockStatus(StorageLevel.DISK_ONLY, a, b)

  def makeBlockManagerId(a: Int, b: Int) = BlockManagerId("freedom", "fighter", 3, b)

  def makeTaskEndReason(a: Int, b: Int, c: Int, d: Long, e: Long) = {
    a % 3 match {
      case 0 => FetchFailed(makeBlockManagerId(c * b, a - b), a, b, c)
      case 1 => Success
      case 2 => ExceptionFailure("Big", "mac", Array[StackTraceElement](),
        Some(makeTaskMetrics(d, d + e, e - d, e)))
    }
  }

  def makeJobResult(a: Int, b: Int) = {
    b % 2 match {
      case 0 => JobSucceeded
      case 1 => JobFailed(new Exception(), b)
    }
  }
}
