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

package org.apache.hadoop.yarn.server.resourcemanager.rmapp.monitor;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationTimeoutType;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.util.AbstractLivelinessMonitor;
import org.apache.hadoop.yarn.util.SystemClock;

/**
 * This service will monitor the applications against the lifetime value given.
 * The applications will be killed if it running beyond the given time.
 */
public class RMAppLifetimeMonitor
    extends AbstractLivelinessMonitor<RMAppToMonitor> {

  private static final Logger LOG =
      LoggerFactory.getLogger(RMAppLifetimeMonitor.class);

  private RMContext rmContext;

  public RMAppLifetimeMonitor(RMContext rmContext) {
    super(RMAppLifetimeMonitor.class.getName(), SystemClock.getInstance());
    this.rmContext = rmContext;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    long monitorInterval =
        conf.getLong(YarnConfiguration.RM_APPLICATION_MONITOR_INTERVAL_MS,
            YarnConfiguration.DEFAULT_RM_APPLICATION_MONITOR_INTERVAL_MS);
    if (monitorInterval <= 0) {
      monitorInterval =
          YarnConfiguration.DEFAULT_RM_APPLICATION_MONITOR_INTERVAL_MS;
    }
    setMonitorInterval(monitorInterval);
    setExpireInterval(0); // No need of expire interval for App.
    setResetTimeOnStart(false); // do not reset expire time on restart
    LOG.info("Application lifelime monitor interval set to " + monitorInterval
        + " ms.");
    super.serviceInit(conf);
  }

  @SuppressWarnings("unchecked")
  @Override
  protected synchronized void expire(RMAppToMonitor monitoredAppKey) {
    ApplicationId appId = monitoredAppKey.getApplicationId();
    RMApp app = rmContext.getRMApps().get(appId);
    if (app == null) {
      return;
    }
    String diagnostics = "Application is killed by ResourceManager as it"
        + " has exceeded the lifetime period.";
    // 九师兄 然后这里也要发送一个事件，这个事件是RMAppEventType.KILL 因为下线ResourceManager的时候
    //  无论对方是否下线成功，最终都要保证彻底被杀死
    rmContext.getDispatcher().getEventHandler()
        .handle(new RMAppEvent(appId, RMAppEventType.KILL, diagnostics));
  }

  public void registerApp(ApplicationId appId,
      ApplicationTimeoutType timeoutType, long expireTime) {
    RMAppToMonitor appToMonitor = new RMAppToMonitor(appId, timeoutType);
    register(appToMonitor, expireTime);
  }

  public void unregisterApp(ApplicationId appId,
      ApplicationTimeoutType timeoutType) {
    RMAppToMonitor remove = new RMAppToMonitor(appId, timeoutType);
    unregister(remove);
  }

  public void unregisterApp(ApplicationId appId,
      Set<ApplicationTimeoutType> timeoutTypes) {
    for (ApplicationTimeoutType timeoutType : timeoutTypes) {
      unregisterApp(appId, timeoutType);
    }
  }

  public void updateApplicationTimeouts(ApplicationId appId,
      Map<ApplicationTimeoutType, Long> timeouts) {
    for (Entry<ApplicationTimeoutType, Long> entry : timeouts.entrySet()) {
      ApplicationTimeoutType timeoutType = entry.getKey();
      RMAppToMonitor update = new RMAppToMonitor(appId, timeoutType);
      register(update, entry.getValue());
    }
  }
}