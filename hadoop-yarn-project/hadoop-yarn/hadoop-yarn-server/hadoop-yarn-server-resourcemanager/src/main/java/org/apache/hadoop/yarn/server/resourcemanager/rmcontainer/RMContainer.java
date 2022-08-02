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

package org.apache.hadoop.yarn.server.resourcemanager.rmcontainer;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerReport;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.api.records.SchedulingRequest;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.common.ContainerRequest;
import org.apache.hadoop.yarn.server.scheduler.SchedulerRequestKey;


/**
 * Represents the ResourceManager's view of an application container. See 
 * {@link RMContainerImpl} for an implementation. Containers may be in one
 * of several states, given in {@link RMContainerState}. An RMContainer
 * instance may exist even if there is no actual running container, such as 
 * when resources are being reserved to fill space for a future container 
 * allocation.
 *
 *  负责维护应用程序的整个运行周期，包括维护同一个Application启动的所有运行实例的生命周期
 *  (RMApp)、维护一次运行尝试的整个生命周期(RMAppAttempt)、维护一个Contaner的运行周期
 *  (RMContainer)、维护一个NodeManager的生命周期(RMNode)。
 *
 * RMContainer维护了一个Container的运行周期，包括从创建到运行的整个过程。RM将资源封装成
 * Container发送给应用程序的ApplicationMaster，而ApplicationMaster则会在Container
 * 描述的环境中启动任务，因此，从这个层面上讲，Container和任务的生命周期是一致的(目前YARN
 * 不支持Container的重用,一个Container用完后会立刻释放，将来可能会增加Container的重用机制)。
 *
 * 在YARN中，根据应用程序需求，资源被切分成大小不同的资源块，每份资源的基本信息由Container描述，
 * 而具体的使用状态追踪则是由RMContainer完成。RMContainer是ResoueceManager中用于维护一个
 * Container生命周期的数据结构，它的实现是RMContianerImpl类，该类维护了一个Container状态机，
 * 记录了一个Container可能存在的各个状态以及导致状态转换的事件，当某个事件发生的时候，
 * RMContainerImpl会根据实际情况进行Container状态的转移，同时触发一个行为。
 *
 */
public interface RMContainer extends EventHandler<RMContainerEvent>,
    Comparable<RMContainer> {

  ContainerId getContainerId();

  void setContainerId(ContainerId containerId);

  ApplicationAttemptId getApplicationAttemptId();

  RMContainerState getState();

  Container getContainer();

  Resource getReservedResource();

  NodeId getReservedNode();
  
  SchedulerRequestKey getReservedSchedulerKey();

  Resource getAllocatedResource();

  Resource getLastConfirmedResource();

  NodeId getAllocatedNode();

  SchedulerRequestKey getAllocatedSchedulerKey();

  Priority getAllocatedPriority();

  long getCreationTime();

  long getFinishTime();

  String getDiagnosticsInfo();

  String getLogURL();

  int getContainerExitStatus();

  ContainerState getContainerState();
  
  ContainerReport createContainerReport();
  
  boolean isAMContainer();

  ContainerRequest getContainerRequest();

  String getNodeHttpAddress();

  Map<String, List<Map<String, String>>> getExposedPorts();

  void setExposedPorts(Map<String, List<Map<String, String>>> exposed);
  
  String getNodeLabelExpression();

  String getQueueName();

  ExecutionType getExecutionType();

  /**
   * If the container was allocated by a container other than the Resource
   * Manager (e.g., the distributed scheduler in the NM
   * <code>LocalScheduler</code>).
   * @return If the container was allocated remotely.
   */
  boolean isRemotelyAllocated();

  /*
   * Return reserved resource for reserved containers, return allocated resource
   * for other container
   */
  Resource getAllocatedOrReservedResource();

  boolean completed();

  NodeId getNodeId();

  /**
   * Return {@link SchedulingRequest#getAllocationTags()} specified by AM.
   * @return allocation tags, could be null/empty
   */
  Set<String> getAllocationTags();
}
