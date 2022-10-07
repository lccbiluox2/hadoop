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

package org.apache.hadoop.yarn.server.nodemanager.containermanager.monitor;

import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceUtilization;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.nodemanager.ResourceView;

/***
 * todo: 2022/10/5 下午7:09 lcc 九师兄
 *       监控容器资源使用。如果内存使用超过虚拟内存大小，NM将杀死此容器，防止影响其他容器运行。
 *       只有内存通过此方式限制。CPU则使用Linux的Cgroups。
 */
public interface ContainersMonitor extends Service,
    EventHandler<ContainersMonitorEvent>, ResourceView {
  ResourceUtilization getContainersUtilization();

  float getVmemRatio();

  void subtractNodeResourcesFromResourceUtilization(
      ResourceUtilization resourceUtil);

  /**
   * Utility method to add a {@link Resource} to the
   * {@link ResourceUtilization}.
   * @param containersMonitor Containers Monitor.
   * @param resourceUtil Resource Utilization.
   * @param resource Resource.
   */
  static void increaseResourceUtilization(
      ContainersMonitor containersMonitor, ResourceUtilization resourceUtil,
      Resource resource) {
    float vCores = (float) resource.getVirtualCores();
    int vmem = (int) (resource.getMemorySize()
        * containersMonitor.getVmemRatio());
    resourceUtil.addTo((int)resource.getMemorySize(), vmem, vCores);
  }

  /**
   * Utility method to subtract a {@link Resource} from the
   * {@link ResourceUtilization}.
   * @param containersMonitor Containers Monitor.
   * @param resourceUtil Resource Utilization.
   * @param resource Resource.
   */
  static void decreaseResourceUtilization(
      ContainersMonitor containersMonitor, ResourceUtilization resourceUtil,
      Resource resource) {
    float vCores = (float) resource.getVirtualCores();
    int vmem = (int) (resource.getMemorySize()
        * containersMonitor.getVmemRatio());
    resourceUtil.subtractFrom((int)resource.getMemorySize(), vmem, vCores);
  }

  /**
   * Set the allocated resources for containers.
   * @param resource Resources allocated for the containers.
   */
  void setAllocatedResourcesForContainers(Resource resource);
}
