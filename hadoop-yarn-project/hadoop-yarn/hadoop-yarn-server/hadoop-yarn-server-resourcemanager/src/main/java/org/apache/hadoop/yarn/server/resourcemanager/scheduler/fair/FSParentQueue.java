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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.TreeSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.hadoop.thirdparty.com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.util.resource.Resources;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;

@Private
@Unstable
public class FSParentQueue extends FSQueue {
  private static final Logger LOG = LoggerFactory.getLogger(
      FSParentQueue.class.getName());

  private final List<FSQueue> childQueues = new ArrayList<>();
  private Resource demand = Resources.createResource(0);
  private int runnableApps;

  private ReadWriteLock rwLock = new ReentrantReadWriteLock();
  private Lock readLock = rwLock.readLock();
  private Lock writeLock = rwLock.writeLock();

  public FSParentQueue(String name, FairScheduler scheduler,
      FSParentQueue parent) {
    super(name, scheduler, parent);
  }

  @Override
  public Resource getMaximumContainerAllocation() {
    if (getName().equals("root")) {
      return maxContainerAllocation;
    }
    if (maxContainerAllocation.equals(Resources.unbounded())
        && getParent() != null) {
      return getParent().getMaximumContainerAllocation();
    } else {
      return maxContainerAllocation;
    }
  }

  void addChildQueue(FSQueue child) {
    writeLock.lock();
    try {
      childQueues.add(child);
    } finally {
      writeLock.unlock();
    }
  }

  void removeChildQueue(FSQueue child) {
    writeLock.lock();
    try {
      childQueues.remove(child);
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  void updateInternal() {
    readLock.lock();
    try {
      policy.computeShares(childQueues, getFairShare());
      for (FSQueue childQueue : childQueues) {
        childQueue.getMetrics().setFairShare(childQueue.getFairShare());
        childQueue.updateInternal();
      }
    } finally {
      readLock.unlock();
    }
  }

  void recomputeSteadyShares() {
    readLock.lock();
    try {
      policy.computeSteadyShares(childQueues, getSteadyFairShare());
      for (FSQueue childQueue : childQueues) {
        childQueue.getMetrics()
            .setSteadyFairShare(childQueue.getSteadyFairShare());
        if (childQueue instanceof FSParentQueue) {
          ((FSParentQueue) childQueue).recomputeSteadyShares();
        }
      }
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public Resource getDemand() {
    readLock.lock();
    try {
      return Resource.newInstance(demand.getMemorySize(), demand.getVirtualCores());
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public void updateDemand() {
    // Compute demand by iterating through apps in the queue
    // Limit demand to maxResources
    writeLock.lock();
    try {
      demand = Resources.createResource(0);
      for (FSQueue childQueue : childQueues) {
        childQueue.updateDemand();
        Resource toAdd = childQueue.getDemand();
        demand = Resources.add(demand, toAdd);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Counting resource from " + childQueue.getName() + " " +
              toAdd + "; Total resource demand for " + getName() +
              " now " + demand);
        }
      }
      // Cap demand to maxShare to limit allocation to maxShare
      demand = Resources.componentwiseMin(demand, getMaxShare());
    } finally {
      writeLock.unlock();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("The updated demand for " + getName() + " is " + demand +
          "; the max is " + getMaxShare());
    }    
  }
  
  private QueueUserACLInfo getUserAclInfo(UserGroupInformation user) {
    List<QueueACL> operations = new ArrayList<>();
    for (QueueACL operation : QueueACL.values()) {
      if (hasAccess(operation, user)) {
        operations.add(operation);
      } 
    }
    return QueueUserACLInfo.newInstance(getQueueName(), operations);
  }
  
  @Override
  public List<QueueUserACLInfo> getQueueUserAclInfo(UserGroupInformation user) {
    List<QueueUserACLInfo> userAcls = new ArrayList<>();
    
    // Add queue acls
    userAcls.add(getUserAclInfo(user));
    
    // Add children queue acls
    readLock.lock();
    try {
      for (FSQueue child : childQueues) {
        userAcls.addAll(child.getQueueUserAclInfo(user));
      }
    } finally {
      readLock.unlock();
    }
 
    return userAcls;
  }

  @Override
  public Resource assignContainer(FSSchedulerNode node) {
    Resource assigned = Resources.none();

    // If this queue is over its limit, reject
    // todo: 九师兄  如果不允许分配比如超出配额限制就直接返回了
    if (!assignContainerPreCheck(node)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Assign container precheck for queue " + getName() +
            " on node " + node.getNodeName() + " failed");
      }
      return assigned;
    }

    // Sort the queues while holding a read lock on this parent only.
    // The individual entries are not locked and can change which means that
    // the collection of childQueues can not be sorted by calling Sort().
    // Locking each childqueue to prevent changes would have a large
    // performance impact.
    // We do not have to handle the queue removal case as a queue must be
    // empty before removal. Assigning an application to a queue and removal of
    // that queue both need the scheduler lock.
    TreeSet<FSQueue> sortedChildQueues = new TreeSet<>(policy.getComparator());
    // todo: 九师兄  在对子队列排序时需要持有写锁
    readLock.lock();
    try {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Node " + node.getNodeName() + " offered to parent queue: " +
            getName() + " visiting " + childQueues.size() + " children");
      }
      // FairSharePolicy分配策略：
      // 通过加权公平共享份额来实现调度比较。以外，低于其最低份额的调度计划比刚好达到最低份额的调度计划优先级更高。
      // 在比较低于最小份额的那些调度计划时，参考的标准是低于最小份额的比例。
      // 比如，作业A具有10个任务的最小份额中的8个，而作业B具有100个最小份额中的50个
      // 则接下来调度作业B，因为B拥有50％的最小份额 而A处于占80％

      // 在比较超过最小份额的调度计划时，参考的标准是 运行中任务数/权重
      // 如果所有权重都相等，就把槽位给拥有最少任务数量的作业
      // 否则，拥有更高权重的作业分配更多

      // 对子队列列表按FairSharePolicy来排序

      sortedChildQueues.addAll(childQueues);

      // 在排序和遍历排序后的子队列列表间隙会释放锁
      // 那么这个队列列表在以下情况可能被修改：
      // 1.将一个子队列添加到子队列列表末尾，那么显然这不会影响container分配
      // 2.移除一个子队列，这样可能还挺好，我们就不分配资源给很快被移除的队列

      for (FSQueue child : sortedChildQueues) {
        // 这里有可能是FSParentQueue FSLeafQueue FSAppAttempt，都实现自Schedulable接口
        assigned = child.assignContainer(node);
        if (!Resources.equals(assigned, Resources.none())) {
          break;
        }
      }
    } finally {
      readLock.unlock();
    }
    return assigned;
  }

  @Override
  public List<FSQueue> getChildQueues() {
    readLock.lock();
    try {
      return ImmutableList.copyOf(childQueues);
    } finally {
      readLock.unlock();
    }
  }

  void incrementRunnableApps() {
    writeLock.lock();
    try {
      runnableApps++;
    } finally {
      writeLock.unlock();
    }
  }
  
  void decrementRunnableApps() {
    writeLock.lock();
    try {
      runnableApps--;
    } finally {
      writeLock.unlock();
    }
  }

  @Override
  public int getNumRunnableApps() {
    readLock.lock();
    try {
      return runnableApps;
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public boolean isEmpty() {
    readLock.lock();
    try {
      for (FSQueue queue: childQueues) {
        if (!queue.isEmpty()) {
          return false;
        }
      }
    } finally {
      readLock.unlock();
    }
    return true;
  }

  @Override
  public void collectSchedulerApplications(
      Collection<ApplicationAttemptId> apps) {
    readLock.lock();
    try {
      for (FSQueue childQueue : childQueues) {
        childQueue.collectSchedulerApplications(apps);
      }
    } finally {
      readLock.unlock();
    }
  }
  
  @Override
  public ActiveUsersManager getAbstractUsersManager() {
    // Should never be called since all applications are submitted to LeafQueues
    return null;
  }

  @Override
  public void recoverContainer(Resource clusterResource,
      SchedulerApplicationAttempt schedulerAttempt, RMContainer rmContainer) {
    // TODO Auto-generated method stub
    
  }

  @Override
  protected void dumpStateInternal(StringBuilder sb) {
    sb.append("{Name: " + getName() +
        ", Weight: " + weights +
        ", Policy: " + policy.getName() +
        ", FairShare: " + getFairShare() +
        ", SteadyFairShare: " + getSteadyFairShare() +
        ", MaxShare: " + getMaxShare() +
        ", MinShare: " + minShare +
        ", ResourceUsage: " + getResourceUsage() +
        ", Demand: " + getDemand() +
        ", MaxAMShare: " + maxAMShare +
        ", Runnable: " + getNumRunnableApps() +
        "}");

    for(FSQueue child : getChildQueues()) {
      sb.append(", ");
      child.dumpStateInternal(sb);
    }
  }
}
