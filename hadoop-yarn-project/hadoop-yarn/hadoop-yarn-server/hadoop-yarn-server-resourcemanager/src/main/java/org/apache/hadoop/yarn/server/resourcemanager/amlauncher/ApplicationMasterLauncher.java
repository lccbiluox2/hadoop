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

package org.apache.hadoop.yarn.server.resourcemanager.amlauncher;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.attempt.RMAppAttempt;

/**
 * 7/30/22 4:51 PM 九师兄
 *
 * 注释:
 * ApplicationMasterLauncher 的工作职责, 负责启动 ApplicationMaster
 * ApplicationMasterLauncher 的工作机制, 跟 AsyncDispatcher 差不多
 **/
public class ApplicationMasterLauncher extends AbstractService implements
    EventHandler<AMLauncherEvent> {
  private static final Logger LOG = LoggerFactory.getLogger(
      ApplicationMasterLauncher.class);

  // 九师兄 线程池是执行任务的队列里面的元素就是runable
  private ThreadPoolExecutor launcherPool;
  // 九师兄 消费队列的线程
  private LauncherThread launcherHandlingThread;

  // 九师兄 队列里面是 Runnable 这个runable就是 AMLauncher
  private final BlockingQueue<Runnable> masterEvents
    = new LinkedBlockingQueue<Runnable>();
  
  protected final RMContext context;
  
  public ApplicationMasterLauncher(RMContext context) {
    super(ApplicationMasterLauncher.class.getName());
    this.context = context;
    // 九师兄 构建执行Runnable的线程，就是执行AMLauncher的线程
    // 九师兄 因为用户是可以提交多个任务的，每个任务如果来了，那么就是放到队列
    // 九师兄 然后又线程从队列里面拿出来要启动什么master,flink,spark 等
    // 九师兄 然后交给这个线程去启动
    this.launcherHandlingThread = new LauncherThread();
  }
  
  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    int threadCount = conf.getInt(
        YarnConfiguration.RM_AMLAUNCHER_THREAD_COUNT,
        YarnConfiguration.DEFAULT_RM_AMLAUNCHER_THREAD_COUNT);
    ThreadFactory tf = new ThreadFactoryBuilder()
        .setNameFormat("ApplicationMasterLauncher #%d")
        .build();
    // 九师兄 初始化工作线程池
    launcherPool = new ThreadPoolExecutor(threadCount, threadCount, 1,
        TimeUnit.HOURS, new LinkedBlockingQueue<Runnable>());
    launcherPool.setThreadFactory(tf);

    Configuration newConf = new YarnConfiguration(conf);
    newConf.setInt(CommonConfigurationKeysPublic.
            IPC_CLIENT_CONNECT_MAX_RETRIES_ON_SOCKET_TIMEOUTS_KEY,
        conf.getInt(YarnConfiguration.RM_NODEMANAGER_CONNECT_RETRIES,
            YarnConfiguration.DEFAULT_RM_NODEMANAGER_CONNECT_RETRIES));
    setConfig(newConf);
    super.serviceInit(newConf);
  }

  @Override
  protected void serviceStart() throws Exception {
    // 九师兄 消费队列的线程启动
    launcherHandlingThread.start();
    super.serviceStart();
  }
  
  protected Runnable createRunnableLauncher(RMAppAttempt application, 
      AMLauncherEventType event) {
    // 九师兄 创建 AMLauncher
    Runnable launcher =
        new AMLauncher(context, application, event, getConfig());
    return launcher;
  }

  /**
   * 7/30/22 5:05 PM 九师兄
   *
   * 主释:
   * AML其实是每次接收到一个事件，AML auncherEventType.LAUNCH就封装-一个AML auncher的任务
   **/
  private void launch(RMAppAttempt application) {
    // 九师兄 创建一个线程 AMLauncher
    Runnable launcher = createRunnableLauncher(application, 
        AMLauncherEventType.LAUNCH);
    // 九师兄 放到队列
    masterEvents.add(launcher);
  }
  

  @Override
  protected void serviceStop() throws Exception {
    launcherHandlingThread.interrupt();
    try {
      launcherHandlingThread.join();
    } catch (InterruptedException ie) {
      LOG.info(launcherHandlingThread.getName() + " interrupted during join ", 
          ie);    }
    launcherPool.shutdown();
  }

  private class LauncherThread extends Thread {
    
    public LauncherThread() {
      super("ApplicationMaster Launcher");
    }

    @Override
    public void run() {
      while (!this.isInterrupted()) {
        Runnable toLaunch;
        try {
          // 九师兄 不停的从队列中拿到Runnable ，比如启动Spark的master ,启动flink的master
          toLaunch = masterEvents.take();
          // 九师兄 然后交给线程池去启动，为什么需要线程池呢、。因为需要并发，总不能一个任务提交等着上面
          // 九师兄 一个任务运行完毕才提交
          launcherPool.execute(toLaunch);
        } catch (InterruptedException e) {
          LOG.warn(this.getClass().getName() + " interrupted. Returning.");
          return;
        }
      }
    }
  }    

  private void cleanup(RMAppAttempt application) {
    Runnable launcher = createRunnableLauncher(application, AMLauncherEventType.CLEANUP);
    masterEvents.add(launcher);
  } 
  
  @Override
  public synchronized void  handle(AMLauncherEvent appEvent) {
    AMLauncherEventType event = appEvent.getType();
    RMAppAttempt application = appEvent.getAppAttempt();
    switch (event) {
    case LAUNCH:
      // 九师兄  接收到launch的事件
      launch(application);
      break;
    case CLEANUP:
      cleanup(application);
      break;
    default:
      break;
    }
  }
}
