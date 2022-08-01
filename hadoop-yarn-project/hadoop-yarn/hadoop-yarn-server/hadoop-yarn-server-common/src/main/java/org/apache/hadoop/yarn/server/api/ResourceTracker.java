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
package org.apache.hadoop.yarn.server.api;

import java.io.IOException;

import org.apache.hadoop.io.retry.AtMostOnce;
import org.apache.hadoop.io.retry.Idempotent;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.NodeHeartbeatResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.RegisterNodeManagerResponse;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerRequest;
import org.apache.hadoop.yarn.server.api.protocolrecords.UnRegisterNodeManagerResponse;

/**
 * This is used by the Node Manager to register/nodeHeartbeat/unregister with
 * the ResourceManager.
 */
public interface ResourceTracker {

  /**
   * 8/1/22 7:17 PM 九师兄
   *
   * NodeManger启动后通过RPC函数ResourceTracker#registerNodeManager向RM注册，之后
   * 将被加入到NMLivenessMonitor中进行监控。它必须周期性通过RPC函数ResourceTracker#nodeHeartBeat
   * 向RM汇报心跳以表明自己还活着，如果一段时间内(默认是10min）内为汇报心跳，则RM宣布它已经死亡，
   * 所以正在运行在它上面的Container将被回收。
   *
   **/
  @Idempotent
  RegisterNodeManagerResponse registerNodeManager(
      RegisterNodeManagerRequest request) throws YarnException, IOException;

  @AtMostOnce
  NodeHeartbeatResponse nodeHeartbeat(NodeHeartbeatRequest request)
      throws YarnException, IOException;

  @Idempotent
  UnRegisterNodeManagerResponse unRegisterNodeManager(
      UnRegisterNodeManagerRequest request) throws YarnException, IOException;
}
