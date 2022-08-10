/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.hadoop.yarn.server.nodemanager;

import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.NodeAttributesProvider;
import org.apache.hadoop.yarn.server.nodemanager.nodelabels.NodeLabelsProvider;

/**
 * 8/1/22 4:54 PM 九师兄
 *
 * NodeStatusUpdater是NodeManager与ResourceManager通信的唯一通道。
 * 当NodeManager启动时，该组件向ResourceManager注册，并汇报节点上可用的
 * 资源(该值在运行过程中不再汇报）；之后,该组件周期性与ResourceManager通信，
 * 汇报各个Container的状态更新，包括节点上正运行的Container、已完成的Container等信息，
 * 同时ResouceManager会返回待清理Container列表、待清理应用程序列表、诊断信息、
 * 各种Token等信息。
 *
 * 在NM刚启动时，NodeStatusUpdater 组件会向ResourceManager 注册，发送本节点的
 * 可用资源，以及NodeManager的Web Server 和RPC Server的监听端口。ResourceManager
 * 在注册过程中，向NodeManager发出安全相关的KEY，NodeManager将用这个KEY为
 * ApplicationMaster的Container 请求做认证。后续的NodeManager-ResourceManager 的通信向
 * ResourceManager提供当前Container的更新信息，ApplicationMasgter 新启动的Container信
 * 息，完成的Container信息，等等。
 *
 * 除此之外，ResourceManager可能通过这个组件来通知NodeManager杀死正在运行的
 * Container,
 *
 * 原因可能是:管理员要让一个NM退役时或者因为网络问题要让NM重新同步。
 * 最终，当ResourceManager.上有任何应用程序结束时，ResourceManager 都会向NodeManager
 * 发出信号，要求清理该应用程序在本节点上对应的资源(如内部每个应用程序的数据结构以
 * 及应用程序级别的本地资源)，然后发起应用程序的日志聚集。
 **/
public interface NodeStatusUpdater extends Service {

  /**
   * Schedule a heartbeat to the ResourceManager outside of the normal,
   * periodic heartbeating process. This is typically called when the state
   * of containers on the node has changed to notify the RM sooner.
   */
  void sendOutofBandHeartBeat();

  /**
   * Get the ResourceManager identifier received during registration
   * @return the ResourceManager ID
   */
  long getRMIdentifier();
  
  /**
   * Query if a container has recently completed
   * @param containerId the container ID
   * @return true if the container has recently completed
   */
  public boolean isContainerRecentlyStopped(ContainerId containerId);
  
  /**
   * Add a container to the list of containers that have recently completed
   * @param containerId the ID of the completed container
   */
  public void addCompletedContainer(ContainerId containerId);

  /**
   * Clear the list of recently completed containers
   */
  public void clearFinishedContainersFromCache();

  /**
   * Report an unrecoverable exception.
   * @param ex exception that makes the node unhealthy
   */
  void reportException(Exception ex);

  /**
   * Sets a node attributes provider to node manager.
   * @param provider
   */
  void setNodeAttributesProvider(NodeAttributesProvider provider);

  /**
   * Sets a node labels provider to the node manager.
   * @param provider
   */
  void setNodeLabelsProvider(NodeLabelsProvider provider);
}
