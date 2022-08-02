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

/**
 * 8/1/22 6:57 PM 九师兄
 *
 * 当一个ApplicationMaster获得一个Container后，YARN不允许ApplicationMaster长时间
 * 不对其使用，这样会降低整个集群的利用率。当ApplicationMaster收到RM新分配的一个Container
 * 后，必须再一定的时间内（默认为10min）内在对应的NM上启动该Container，否则RM将强制回收该
 * Container，而一个已经分配的Container是否被回收则是由ContinerAllocationExpier决定和
 * 执行的。
 *
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
    dispatcher.handle(new ContainerExpiredSchedulerEvent(
        allocationExpirationInfo.getContainerId(),
            allocationExpirationInfo.isIncrease()));
  }
}
