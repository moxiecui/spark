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

import java.io.InputStream

import scala.io.Source

import it.unimi.dsi.fastutil.io.FastBufferedInputStream
import org.apache.hadoop.fs.{Path, FileSystem}
import org.json4s.jackson.JsonMethods._

import org.apache.spark.Logging
import org.apache.spark.util.{Utils, JsonProtocol}

/**
 * An EventBus that replays logged events from persisted storage.
 */
private[spark] class SparkReplayerBus extends EventBus with Logging {

  /**
   * Return a list of paths representing log files in the given directory.
   */
  private def getLogFilePaths(logDir: String, fileSystem: FileSystem): Array[Path] = {
    val path = new Path(logDir)
    if (!fileSystem.exists(path) || !fileSystem.getFileStatus(path).isDir) {
      logWarning("Log path provided is not a valid directory: %s".format(logDir))
      return Array[Path]()
    }
    val logStatus = fileSystem.listStatus(path)
    if (logStatus == null || !logStatus.exists(!_.isDir)) {
      logWarning("Log path provided contains no log files: %s".format(logDir))
      return Array[Path]()
    }
    logStatus.filter(!_.isDir).map(_.getPath).sortBy(_.getName)
  }

  /**
   * Replay each event in the order maintained in the given logs.
   */
  def replay(logDir: String): Boolean = {
    val fileSystem = Utils.getHadoopFileSystem(logDir)
    val logPaths = getLogFilePaths(logDir, fileSystem)
    if (logPaths.length == 0) {
      return false
    }

    logPaths.foreach { path =>
      // In case there is an exception, keep track of the highest level stream to close it later
      var streamToClose: Option[InputStream] = None
      var currentLine = ""
      try {
        val fstream = fileSystem.open(path)
        val bstream = new FastBufferedInputStream(fstream)
        streamToClose = Some(bstream)

        // Parse each line as an event and post it to all attached listeners
        val lines = Source.fromInputStream(bstream).getLines()
        lines.foreach { line =>
          currentLine = line
          val event = JsonProtocol.sparkEventFromJson(parse(line))
          postToAll(event)
        }
      } catch {
        case e: Exception =>
          logWarning("Exception in parsing UI logs for %s".format(path))
          logWarning(currentLine + "\n")
          logDebug(e.getMessage + e.getStackTraceString)
      } finally {
        streamToClose.foreach(_.close())
      }
    }
    fileSystem.close()
    true
  }

}
