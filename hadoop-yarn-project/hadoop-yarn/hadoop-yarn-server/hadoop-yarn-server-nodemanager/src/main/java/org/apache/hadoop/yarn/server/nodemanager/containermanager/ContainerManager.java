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

package org.apache.hadoop.yarn.server.nodemanager.containermanager;

import org.apache.hadoop.service.ServiceStateChangeListener;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.api.records.ContainerQueuingLimit;
import org.apache.hadoop.yarn.server.api.records.OpportunisticContainersStatus;
import org.apache.hadoop.yarn.server.nodemanager.ContainerManagerEvent;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.localizer.ResourceLocalizationService;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor
    .ContainersMonitor;
import org.apache.hadoop.yarn.server.nodemanager.containermanager.scheduler
    .ContainerScheduler;

/**
 * The ContainerManager is an entity that manages the life cycle of Containers.
 *
 * ContainerManager是NodeManager中最新核心的组件之一，它由多个子组件构成，每个子组件负责
 * 一部分功能，它们协同工作组件用来管理运行在该节点上的所有Container,其主要包含的组件如下：
 *
 * 1. RPCServer  实现了ContainerManagementProtocol协议，是AM和NM通信的唯一通道。
 *              ContainerManager从各个ApplicationMaster上接受RPC请求以启动新的
 *              Container或者停止正在运行的Contaier。
 * 2. ResourceLocalizationService 负责Container所需资源的本地化。能够按照描述从
 *            HDFS上下载Container所需的文件资源，并尽量将他们分摊到各个磁盘上以防止
 *            出现访问热点。此外，它会为下载的文件添加访问控制权限，并为之施加合适的磁
 *            盘空间使用份额。
 * 3. ContainerLaucher 维护了一个线程池以并行完成Container相关操作。比如杀死或启动
 *            Container。 启动请求由AM发起，杀死请求有AM或者RM发起。
 *
 * 4. AuxServices  NodeManager允许用户通过配置附属服务的方式扩展自己的功能，这使得每个
 *           节点可以定制一些特定框架需要的服务。附属服务需要在NM启动前配置好，并由NM统一
 *           启动和关闭。典型的应用是MapReduce框架中用到的Shuffle HTTP Server，其通过
 *           封装成一个附属服务由各个NodeManager启动。
 *
 * 5. ContainersMonitor 负责监控Container的资源使用量。为了实现资源的隔离和公平共享，RM
 *          为每个Container分配一定量的资源，而ContainerMonitor周期性的探测它在运行过程
 *          中的资源利用量，一旦发现Container超过它允许使用的份额上限，就向它发送信号将其杀死。这可以避免资源密集型的Container影响到同节点上的其他正在运行的Container。
 *
 * 注：YARN只有对内存资源是通过ContainerMonitor监控的方式加以限制的，对于CPU资源，则采用
 * 轻量级资源隔离方案Cgroups.
 */
public interface ContainerManager extends ServiceStateChangeListener,
    ContainerManagementProtocol,
    EventHandler<ContainerManagerEvent> {

  ContainersMonitor getContainersMonitor();

  OpportunisticContainersStatus getOpportunisticContainersStatus();

  void updateQueuingLimit(ContainerQueuingLimit queuingLimit);

  ContainerScheduler getContainerScheduler();

  void handleCredentialUpdate();

  ResourceLocalizationService getResourceLocalizationService();

}
