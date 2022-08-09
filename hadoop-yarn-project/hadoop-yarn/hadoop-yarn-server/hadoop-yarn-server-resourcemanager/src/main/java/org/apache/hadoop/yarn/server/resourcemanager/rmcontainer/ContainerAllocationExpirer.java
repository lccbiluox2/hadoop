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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;

/***
 * 2022/8/9 下午10:11 lcc 九师兄
 * todo: 8/1/22 6:57 PM 九师兄
 *
 *todo:当一个ApplicationMaster获得一个Container后，YARN不允许ApplicationMaster长时间
 * 不对其使用，这样会降低整个集群的利用率。当ApplicationMaster收到RM新分配的一个Container
 * 后，必须再一定的时间内（默认为10min）内在对应的NM上启动该Container，否则RM将强制回收该
 * Container，而一个已经分配的Container是否被回收则是由ContinerAllocationExpier决定和
 * 执行的。
 *
 * 该组件负责确保所有分配的Container最终被ApplicationMaster使用，并在相应的
 * NodeManager上拉起。ApplicationMaster 运行着非可信任的用户代码，可能拿到分配的
 * Container而不使用它们。这样，将降低资源使用率，浪费集群资源。为解决这个问题，
 * ContainerAllocationExpirer包含了一个已分配但是还没有在相应NodeManager上启动的
 * Container的列表。对任意的Container,在一个配置的时间间隔(默认10分钟)，如果相应的
 * NodeManager没有报告给ResourceManager该Container已经运行了，在ResourceManager中
 * 该容器被当作死亡的且超时。
 *
 * 此外，NodeManager自己也查看这个超时时间，该信息编码绑定在Container的
 * ContainerToken中。如果Container已经超时，则NodeManager拒绝拉起该Container。显然，
 * 该功能依赖于系统中ResourceManager和NodeManager上系统时钟的同步。
 **/
@SuppressWarnings({"unchecked", "rawtypes"})
public class ContainerAllocationExpirer extends
    AbstractLivelinessMonitor<AllocationExpirationInfo> {

  private EventHandler dispatcher;

  public ContainerAllocationExpirer(Dispatcher d) {
    super(ContainerAllocationExpirer.class.getName());
    this.dispatcher = d.getEventHandler();
  }

  public void serviceInit(Configuration conf) throws Exception {
    int expireIntvl = conf.getInt(
            YarnConfiguration.RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_CONTAINER_ALLOC_EXPIRY_INTERVAL_MS);
    setExpireInterval(expireIntvl);
    setMonitorInterval(expireIntvl/3);
    super.serviceInit(conf);
  }

  @Override
  protected void expire(AllocationExpirationInfo allocationExpirationInfo) {
    //  todo: 下午10:11 九师兄 过期了就发送一个过期处理事件
    dispatcher.handle(new ContainerExpiredSchedulerEvent(
        allocationExpirationInfo.getContainerId(),
            allocationExpirationInfo.isIncrease()));
  }
}
