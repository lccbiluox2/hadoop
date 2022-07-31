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

package org.apache.hadoop.yarn.server.resourcemanager;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNodeEventType;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;

public class NMLivelinessMonitor extends AbstractLivelinessMonitor<NodeId> {

  private EventHandler<Event> dispatcher;
  
  public NMLivelinessMonitor(Dispatcher d) {
    super("NMLivelinessMonitor");
    this.dispatcher = d.getEventHandler();
  }

  /**
   * 7/31/22 10:20 AM 九师兄
   * 注释:
   * 验活机制
   * 1、其实就是设计一个for循环，每隔一段时间， 针对注册成功的NodeManager 执行心跳超时判断
   * 2、如果发现某个nodemanager 的上一次心跳时间距离现在超过了规定的超时时间，则认为超时了，进行超时处理
   * 3、超时处理，就是调用: expire()方法
   * 步骤1和步骤2这两件事，其实是通用的，抽象出来这个工作机制，放在父类中做实现，
   * 具体的超时处理逻辑，有expire() 方法去完成，具体实现逻辑就交给 子类
   **/
  public void serviceInit(Configuration conf) throws Exception {
    int expireIntvl = conf.getInt(YarnConfiguration.RM_NM_EXPIRY_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_NM_EXPIRY_INTERVAL_MS);
    setExpireInterval(expireIntvl);
    setMonitorInterval(expireIntvl/3);
    super.serviceInit(conf);
  }

  @Override
  protected void expire(NodeId id) {
    // 九师兄 todo: 这里发送了一个RMNodeEvent
    dispatcher.handle(
        new RMNodeEvent(id, RMNodeEventType.EXPIRE));
    // 10:27 AM  九师兄 这个事件最终是 RMNode 这个状态机进行处理
    // 10:28 AM  九师兄
    // 方式1:
    //   构建一个关键词: RMNodeEvent . class
    //   找到一个类似的代码: xXx. register (RMNodeEvent . cLass, XXEventHandLer)
    // 方式2:搜索具体的事件，关键词: RMNodeEventType . EXPIRE
    //     addTransition (NodeState . RUNNING, NodeState.LOST, RMNodeEventType . EXPIRE ,
    //           new DeactivateNodeTransition(NodeState.LOST)
    //     去看DeactivateNodeTransition 的transition( )
  }
}