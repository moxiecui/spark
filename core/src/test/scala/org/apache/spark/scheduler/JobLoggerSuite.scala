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

package org.apache.spark.scheduler

import org.scalatest.FunSuite
import org.scalatest.matchers.ShouldMatchers

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

class JobLoggerSuite extends FunSuite with LocalSparkContext with ShouldMatchers {
  val WAIT_TIMEOUT_MILLIS = 10000

  test("inner method") {
    sc = new SparkContext("local", "joblogger")
    val joblogger = new JobLogger {
      def createLogWriterTest(jobId: Int) = createLogWriter(jobId)
      def closeLogWriterTest(jobId: Int) = closeLogWriter(jobId)
    }
    type MyRDD = RDD[(Int, Int)]
    def makeRdd(numPartitions: Int, dependencies: List[Dependency[_]]): MyRDD = {
      val maxPartition = numPartitions - 1
      new MyRDD(sc, dependencies) {
        override def compute(split: Partition, context: TaskContext): Iterator[(Int, Int)] =
          throw new RuntimeException("should not be reached")
        override def getPartitions = (0 to maxPartition).map(i => new Partition {
          override def index = i
        }).toArray
      }
    }
    val jobId = 5
    val parentRdd = makeRdd(4, Nil)
    val shuffleDep = new ShuffleDependency(parentRdd, null)
    val rootRdd = makeRdd(4, List(shuffleDep))
    val shuffleMapStage =
      new Stage(1, parentRdd, parentRdd.partitions.size, Some(shuffleDep), Nil, jobId, None)
    val rootStage =
      new Stage(0, rootRdd, rootRdd.partitions.size, None, List(shuffleMapStage), jobId, None)
    val rootStageInfo = StageInfo.fromStage(rootStage)

    joblogger.onJobStart(SparkListenerJobStart(jobId, Seq[Int](0, 1)))
    joblogger.onStageSubmitted(SparkListenerStageSubmitted(rootStageInfo))
    joblogger.createLogWriterTest(jobId)
    joblogger.getJobIdToPrintWriter.size should be (1)
    joblogger.getJobIdToStageIds.get(jobId).get.size should be (2)
    joblogger.getStageIdToJobId.get(0) should be (Some(jobId))
    joblogger.getStageIdToJobId.get(1) should be (Some(jobId))
    joblogger.closeLogWriterTest(jobId)
    joblogger.getStageIdToJobId.size should be (0)
    joblogger.getJobIdToStageIds.size should be (0)
    joblogger.getJobIdToPrintWriter.size should be (0)
  }
  
  test("inner variables") {
    sc = new SparkContext("local[4]", "joblogger")
    val joblogger = new JobLogger {
      override protected def closeLogWriter(jobId: Int) =
        getJobIdToPrintWriter.get(jobId).foreach { fileWriter =>
          fileWriter.close()
        }
    }
    sc.addSparkListener(joblogger)
    val rdd = sc.parallelize(1 to 1e2.toInt, 4).map{ i => (i % 12, 2 * i) }
    rdd.reduceByKey(_+_).collect()

    assert(sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS))

    val user = System.getProperty("user.name",  SparkContext.SPARK_UNKNOWN_USER)
    
    joblogger.getLogDir should be ("/tmp/spark-%s".format(user))
    joblogger.getJobIdToPrintWriter.size should be (1)
    joblogger.getStageIdToJobId.size should be (2)
    joblogger.getStageIdToJobId.get(0) should be (Some(0))
    joblogger.getStageIdToJobId.get(1) should be (Some(0))
    joblogger.getJobIdToStageIds.size should be (1)
  }
  
  
  test("interface functions") {
    sc = new SparkContext("local[4]", "joblogger")
    val joblogger = new JobLogger {
      var onTaskEndCount = 0
      var onJobEndCount = 0 
      var onJobStartCount = 0
      var onStageCompletedCount = 0
      var onStageSubmittedCount = 0
      override def onTaskEnd(taskEnd: SparkListenerTaskEnd)  = onTaskEndCount += 1
      override def onJobEnd(jobEnd: SparkListenerJobEnd) = onJobEndCount += 1
      override def onJobStart(jobStart: SparkListenerJobStart) = onJobStartCount += 1
      override def onStageCompleted(stageCompleted: SparkListenerStageCompleted) = onStageCompletedCount += 1
      override def onStageSubmitted(stageSubmitted: SparkListenerStageSubmitted) = onStageSubmittedCount += 1
    }
    sc.addSparkListener(joblogger)
    val rdd = sc.parallelize(1 to 1e2.toInt, 4).map{ i => (i % 12, 2 * i) }
    rdd.reduceByKey(_+_).collect()

    assert(sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS))

    joblogger.onJobStartCount should be (1)
    joblogger.onJobEndCount should be (1)
    joblogger.onTaskEndCount should be (8)
    joblogger.onStageSubmittedCount should be (2)
    joblogger.onStageCompletedCount should be (2)
  }
}
