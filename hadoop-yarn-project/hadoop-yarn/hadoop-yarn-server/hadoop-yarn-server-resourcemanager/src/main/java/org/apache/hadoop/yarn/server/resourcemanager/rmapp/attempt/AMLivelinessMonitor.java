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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;
import org.apache.hadoop.yarn.util.Clock;

/**
 * 8/1/22 6:41 PM 九师兄
 *
 *  监控ApplicationMaster是否还活着，如果一个ApplicationMaster在一定时间(10min)
 *  内未汇报心跳信息，则认为它死掉了，它上面的所有正在运行的Container将被设置为失败状态，
 *  而ApplicationMaster本身会被重新分配到另一个节点上(默认尝试运行2次）。
 *
 **/
public class AMLivelinessMonitor extends AbstractLivelinessMonitor<ApplicationAttemptId> {

  private EventHandler<Event> dispatcher;
  
  public AMLivelinessMonitor(Dispatcher d) {
    super("AMLivelinessMonitor");
    this.dispatcher = d.getEventHandler();
  }

  public AMLivelinessMonitor(Dispatcher d, Clock clock) {
    super("AMLivelinessMonitor", clock);
    this.dispatcher = d.getEventHandler();
  }

  public void serviceInit(Configuration conf) throws Exception {
    super.serviceInit(conf);
    int expireIntvl = conf.getInt(YarnConfiguration.RM_AM_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_AM_EXPIRY_INTERVAL_MS);
    setExpireInterval(expireIntvl);
    setMonitorInterval(expireIntvl/3);
  }

  @Override
  protected void expire(ApplicationAttemptId id) {
    // 九师兄 这里是放入了一个事件是RMAppAttemptEvent
    dispatcher.handle(
        new RMAppAttemptEvent(id, RMAppAttemptEventType.EXPIRE));
  }
}