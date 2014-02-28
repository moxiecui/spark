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

package org.apache.spark.ui.env

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.eclipse.jetty.server.Handler

import org.apache.spark.scheduler._
import org.apache.spark.ui.JettyUtils._
import org.apache.spark.ui.Page.Environment
import org.apache.spark.ui._

private[ui] class EnvironmentUI(parent: SparkUI) {
  lazy val appName = parent.appName
  lazy val listener = _listener.get
  val live = parent.live
  val sc = parent.sc

  private var _listener: Option[EnvironmentListener] = None

  def start() {
    val gateway = parent.gatewayListener
    _listener = Some(new EnvironmentListener())
    gateway.registerSparkListener(listener)
  }

  def getHandlers = Seq[(String, Handler)](
    ("/environment", (request: HttpServletRequest) => render(request))
  )

  def render(request: HttpServletRequest): Seq[Node] = {
    val runtimeInformationTable = UIUtils.listingTable(
      propertyHeader, jvmRow, listener.jvmInformation, fixedWidth = true)
    val sparkPropertiesTable = UIUtils.listingTable(
      propertyHeader, propertyRow, listener.sparkProperties, fixedWidth = true)
    val systemPropertiesTable = UIUtils.listingTable(
      propertyHeader, propertyRow, listener.systemProperties, fixedWidth = true)
    val classpathEntriesTable = UIUtils.listingTable(
      classPathHeaders, classPathRow, listener.classpathEntries, fixedWidth = true)
    val content =
      <span>
        <h4>Runtime Information</h4> {runtimeInformationTable}
        <h4>Spark Properties</h4> {sparkPropertiesTable}
        <h4>System Properties</h4> {systemPropertiesTable}
        <h4>Classpath Entries</h4> {classpathEntriesTable}
      </span>

    UIUtils.headerSparkPage(content, appName, "Environment", Environment)
  }

  private def propertyHeader = Seq("Name", "Value")
  private def classPathHeaders = Seq("Resource", "Source")
  private def jvmRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
  private def propertyRow(kv: (String, String)) = <tr><td>{kv._1}</td><td>{kv._2}</td></tr>
  private def classPathRow(data: (String, String)) = <tr><td>{data._1}</td><td>{data._2}</td></tr>
}

/**
 * A SparkListener that prepares information to be displayed on the EnvironmentUI
 */
private[ui] class EnvironmentListener extends UISparkListener {
  var jvmInformation: Seq[(String, String)] = Seq()
  var sparkProperties: Seq[(String, String)] = Seq()
  var systemProperties: Seq[(String, String)] = Seq()
  var classpathEntries: Seq[(String, String)] = Seq()

  override def onEnvironmentUpdate(environmentUpdate: SparkListenerEnvironmentUpdate) {
    val environmentDetails = environmentUpdate.environmentDetails
    jvmInformation = environmentDetails("JVM Information")
    sparkProperties = environmentDetails("Spark Properties")
    systemProperties = environmentDetails("System Properties")
    classpathEntries = environmentDetails("Classpath Entries")
  }
}
