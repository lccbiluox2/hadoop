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

package org.apache.hadoop.service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the base implementation class for services.
 */
@Public
@Evolving
public abstract class AbstractService implements Service {

  private static final Logger LOG =
      LoggerFactory.getLogger(AbstractService.class);

  /**
   * Service name.
   */
  // todo: 九师兄  注意服务名用final修饰，一旦指定后不可改变
  private final String name;

  /** service state */
  // todo: 九师兄  封装了服务状态的模型类
  private final ServiceStateModel stateModel;

  /**
   * Service start time. Will be zero until the service is started.
   */
  private long startTime;

  /**
   * The configuration. Will be null until the service is initialized.
   */
  private volatile Configuration config;

  /**
   * List of state change listeners; it is final to ensure
   * that it will never be null.
   */
  // todo: 九师兄  服务状态变化的监听器，
  private final ServiceOperations.ServiceListeners listeners
    = new ServiceOperations.ServiceListeners();
  /**
   * Static listeners to all events across all services
   */
  // todo: 5Lmd5biI5YWE  注意和上面的监听器区分，这个是全局的、横跨所有服务的监听器
  private static ServiceOperations.ServiceListeners globalListeners
    = new ServiceOperations.ServiceListeners();

  /**
   * The cause of any failure -will be null.
   * if a service did not stop due to a failure.
   */
  private Exception failureCause;

  /**
   * the state in which the service was when it failed.
   * Only valid when the service is stopped due to a failure
   */
  // todo: 九师兄  服务失败时处于的状态，仅当服务因为一个错误导致失败时合法
  private STATE failureState = null;

  /**
   * object used to co-ordinate {@link #waitForServiceToStop(long)}
   * across threads.
   */
  // todo: 九师兄  用来在多线程中协助 waitForServiceToStop方法，为true代表该服务已经终止
  private final AtomicBoolean terminationNotification =
    new AtomicBoolean(false);

  /**
   * History of lifecycle transitions
   */
  // todo: lcc  生命周期历史组成的list
  private final List<LifecycleEvent> lifecycleHistory
    = new ArrayList<LifecycleEvent>(5);

  /**
   * Map of blocking dependencies
   */
  private final Map<String,String> blockerMap = new HashMap<String, String>();

  // todo: 九师兄  状态转义时用来做对象锁
  private final Object stateChangeLock = new Object();
 
  /**
   * Construct the service.
   * @param name service name
   */
  public AbstractService(String name) {
    this.name = name;
    stateModel = new ServiceStateModel(name);
  }

  @Override
  public final STATE getServiceState() {
    return stateModel.getState();
  }

  @Override
  public final synchronized Throwable getFailureCause() {
    return failureCause;
  }

  @Override
  public synchronized STATE getFailureState() {
    return failureState;
  }

  /**
   * Set the configuration for this service.
   * This method is called during {@link #init(Configuration)}
   * and should only be needed if for some reason a service implementation
   * needs to override that initial setting -for example replacing
   * it with a new subclass of {@link Configuration}
   *
   * 设置此服务的配置。这个方法在{@link #init(Configuration)}期间被调用，并且只在由于
   * 某些原因服务实现需要重写初始设置时才需要-例如用{@link Configuration}的一个
   * 新子类替换它
   *
   * @param conf new configuration.
   */
  protected void setConfig(Configuration conf) {
    this.config = conf;
  }

  /**
   * {@inheritDoc}
   * This invokes {@link #serviceInit}
   * @param conf the configuration of the service. This must not be null
   * @throws ServiceStateException if the configuration was null,
   * the state change not permitted, or something else went wrong
   */
  // todo: 九师兄  重写自Service类，会调用 serviceInit方法
  // 代码中可以看到，该方法如果多次反复调用，除了第一次以外的调用都不会起作用
  // 也就是说该方法不可重入
  // configuration参数为空、状态转移非法或者是其他出错时，会抛出ServiceStateException
  @Override
  public void init(Configuration conf) {
    if (conf == null) {
      throw new ServiceStateException("Cannot initialize service "
                                      + getName() + ": null configuration");
    }
    if (isInState(STATE.INITED)) {
      return;
    }
    synchronized (stateChangeLock) {
      if (enterState(STATE.INITED) != STATE.INITED) {
        setConfig(conf);
        try {
          // todo: 调用 serviceInit 初始化
          // 这里调用的是 v2.app.MRAppMaster.serviceInit
          serviceInit(config);
          if (isInState(STATE.INITED)) {
            //if the service ended up here during init,
            //notify the listeners
            // 通知相关的监听器
            notifyListeners();
          }
        } catch (Exception e) {
          noteFailure(e);
          ServiceOperations.stopQuietly(LOG, this);
          throw ServiceStateException.convert(e);
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   * @throws ServiceStateException if the current service state does not permit
   * this action
   */
  @Override
  public void start() {
    if (isInState(STATE.STARTED)) {
      return;
    }
    //enter the started state
    synchronized (stateChangeLock) {
      if (stateModel.enterState(STATE.STARTED) != STATE.STARTED) {
        try {
          startTime = System.currentTimeMillis();
          // todo: 启动一些服务 代理方法 所以还是 自己的方法
          // 这里是 MRAppMaster.ContainerAllocatorRouter.serviceStart
          serviceStart();
          if (isInState(STATE.STARTED)) {
            //if the service started (and isn't now in a later state), notify
            LOG.debug("Service {} is started", getName());
            notifyListeners();
          }
        } catch (Exception e) {
          noteFailure(e);
          ServiceOperations.stopQuietly(LOG, this);
          throw ServiceStateException.convert(e);
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   *
   * 重写自Service类，会调用serviceStart方法
   *   // 当前服务状态不允许start时，会抛出ServiceStateException
   */
  @Override
  public void stop() {
    if (isInState(STATE.STOPPED)) {
      return;
    }
    synchronized (stateChangeLock) {
      if (enterState(STATE.STOPPED) != STATE.STOPPED) {
        try {
          serviceStop();
        } catch (Exception e) {
          //stop-time exceptions are logged if they are the first one,
          noteFailure(e);
          throw ServiceStateException.convert(e);
        } finally {
          //report that the service has terminated
          // todo: 九师兄  最终记录改服务已经终止
          terminationNotification.set(true);
          // todo: 九师兄  唤醒阻塞在terminationNotification上面的所有线程
          synchronized (terminationNotification) {
            terminationNotification.notifyAll();
          }
          //notify anything listening for events
          notifyListeners();
        }
      } else {
        //already stopped: note it
        LOG.debug("Ignoring re-entrant call to stop()");
      }
    }
  }

  /**
   * Relay to {@link #stop()}
   * @throws IOException
   */
  @Override
  public final void close() throws IOException {
    stop();
  }

  /**
   * Failure handling: record the exception
   * that triggered it -if there was not one already.
   * Services are free to call this themselves.
   * @param exception the exception
   */
  // todo: 九师兄  记录触发该方法的异常
  protected final void noteFailure(Exception exception) {
    LOG.debug("noteFailure", exception);
    if (exception == null) {
      //make sure failure logic doesn't itself cause problems
      return;
    }
    //record the failure details, and log it
    synchronized (this) {
      if (failureCause == null) {
        failureCause = exception;
        failureState = getServiceState();
        LOG.info("Service {} failed in state {}",
            getName(), failureState, exception);
      }
    }
  }

  // todo: 九师兄  等待服务在指定时间内终止
  @Override
  public final boolean waitForServiceToStop(long timeout) {
    boolean completed = terminationNotification.get();
    // todo: 九师兄  当服务未完成的时候，就等待指定timeout然后判断是否完成
    while (!completed) {
      try {
        // todo: 九师兄 获取terminationNotification对象锁
        synchronized(terminationNotification) {
          terminationNotification.wait(timeout);
        }
        // here there has been a timeout, the object has terminated,
        // or there has been a spurious wakeup (which we ignore)
        completed = true;
      } catch (InterruptedException e) {
        // interrupted; have another look at the flag
        completed = terminationNotification.get();
      }
    }
    // todo: 九师兄  最后返回当前是否服务终止
    return terminationNotification.get();
  }

  /* ===================================================================== */
  /* Override Points */
  /* ===================================================================== */

  /**
   * All initialization code needed by a service.
   *
   * This method will only ever be called once during the lifecycle of
   * a specific service instance.
   *
   * Implementations do not need to be synchronized as the logic
   * in {@link #init(Configuration)} prevents re-entrancy.
   *
   * The base implementation checks to see if the subclass has created
   * a new configuration instance, and if so, updates the base class value
   * @param conf configuration
   * @throws Exception on a failure -these will be caught,
   * possibly wrapped, and will trigger a service stop
   */
  /**
   *todo: 8/7/22 9:44 AM 九师兄
   * 服务所需的所有初始化代码
   *
   * 这个方法仅会在特定的service类实例生命周期内被调用一次
   *
   * 方法实现中不需要使用synchronized，因为init方法内已阻止了方法重入
   *
   * 基本的实现是检查传入的conf参数和现有config对象是否一致，不一致就更新现有config
   */
  protected void serviceInit(Configuration conf) throws Exception {
    if (conf != config) {
      LOG.debug("Config has been overridden during init");
      setConfig(conf);
    }
  }

  /**
   * Actions called during the INITED to STARTED transition.
   *
   * This method will only ever be called once during the lifecycle of
   * a specific service instance.
   *
   * Implementations do not need to be synchronized as the logic
   * in {@link #start()} prevents re-entrancy.
   *
   * @throws Exception if needed -these will be caught,
   * wrapped, and trigger a service stop
   */
  /**
   *todo: 8/7/22 9:44 AM 九师兄
   * 在 INITED->STARTED 转移时调用
   *
   * 这个方法仅会在特定的service类实例生命周期内被调用一次
   *
   * 方法实现中不需要使用synchronized，因为start方法内已阻止了方法重入
   *
   * 按需抛出异常，被调用者捕获后然后触发服务停止
   */
  protected void serviceStart() throws Exception {

  }

  /**
   * Actions called during the transition to the STOPPED state.
   *
   * This method will only ever be called once during the lifecycle of
   * a specific service instance.
   *
   * Implementations do not need to be synchronized as the logic
   * in {@link #stop()} prevents re-entrancy.
   *
   * Implementations MUST write this to be robust against failures, including
   * checks for null references -and for the first failure to not stop other
   * attempts to shut down parts of the service.
   *
   * @throws Exception if needed -these will be caught and logged.
   */
  /**
   *todo: 8/7/22 9:44 AM 九师兄
   * 在 状态向 STARTED 转移时调用
   *
   * 这个方法仅会在特定的service类实例生命周期内被调用一次
   *
   * 方法实现中不需要使用synchronized，因为stop方法内已阻止了方法重入
   *
   * 实现该方法的代码在失败处理方面必须是十分健壮的，包括检查空引用等
   *
   * 按需抛出异常，被调用者捕获后会记录日志
   */
  protected void serviceStop() throws Exception {

  }

  @Override
  public void registerServiceListener(ServiceStateChangeListener l) {
    listeners.add(l);
  }

  @Override
  public void unregisterServiceListener(ServiceStateChangeListener l) {
    listeners.remove(l);
  }

  /**
   * Register a global listener, which receives notifications
   * from the state change events of all services in the JVM
   * @param l listener
   */
  // todo: 九师兄  注册全局的监听JVM所有服务状态变更的监听器。注意和前面的实例监听器区分
  public static void registerGlobalListener(ServiceStateChangeListener l) {
    globalListeners.add(l);
  }

  /**
   * unregister a global listener.
   * @param l listener to unregister
   * @return true if the listener was found (and then deleted)
   */
  // todo: 九师兄 注销一个全局监听器，当找到时就返回true，然后注销
  public static boolean unregisterGlobalListener(ServiceStateChangeListener l) {
    return globalListeners.remove(l);
  }

  /**
   * Package-scoped method for testing -resets the global listener list
   */
  // todo: 九师兄  Package-scoped 方法，测试用：重置全局监听器列表
  @VisibleForTesting
  static void resetGlobalListeners() {
    globalListeners.reset();
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Configuration getConfig() {
    return config;
  }

  @Override
  public long getStartTime() {
    return startTime;
  }

  /**
   * Notify local and global listeners of state changes.
   * Exceptions raised by listeners are NOT passed up.
   */
  // todo: 九师兄  将状态变更通知本地和全局监听器。通知监听器时发生的异常不允许向上传递。
  private void notifyListeners() {
    try {
      listeners.notifyListeners(this);
      globalListeners.notifyListeners(this);
    } catch (Throwable e) {
      LOG.warn("Exception while notifying listeners of {}", this, e);
    }
  }

  /**
   * Add a state change event to the lifecycle history
   */
  // 新增一个状态变更事件到生命周期历史中
  private void recordLifecycleEvent() {
    LifecycleEvent event = new LifecycleEvent();
    event.time = System.currentTimeMillis();
    event.state = getServiceState();
    lifecycleHistory.add(event);
  }

  // todo: 九师兄  重写Service中的该方法，返回生命周期历史组成的list
  @Override
  public synchronized List<LifecycleEvent> getLifecycleHistory() {
    return new ArrayList<LifecycleEvent>(lifecycleHistory);
  }

  /**
   * Enter a state; record this via {@link #recordLifecycleEvent}
   * and log at the info level.
   * @param newState the proposed new state
   * @return the original state
   * it wasn't already in that state, and the state model permits state re-entrancy.
   */
  // 进入指定状态，并会调用recordLifecycleEvent记录转移事件。
  // 返回之前的状态
  private STATE enterState(STATE newState) {
    assert stateModel != null : "null state in " + name + " " + this.getClass();
    STATE oldState = stateModel.enterState(newState);
    if (oldState != newState) {
      LOG.debug("Service: {} entered state {}", getName(), getServiceState());

      recordLifecycleEvent();
    }
    return oldState;
  }

  @Override
  public final boolean isInState(Service.STATE expected) {
    return stateModel.isInState(expected);
  }

  @Override
  public String toString() {
    return "Service " + name + " in state " + stateModel;
  }

  /**
   * Put a blocker to the blocker map -replacing any
   * with the same name.
   * @param name blocker name
   * @param details any specifics on the block. This must be non-null.
   */
  protected void putBlocker(String name, String details) {
    synchronized (blockerMap) {
      blockerMap.put(name, details);
    }
  }

  /**
   * Remove a blocker from the blocker map -
   * this is a no-op if the blocker is not present
   * @param name the name of the blocker
   */
  public void removeBlocker(String name) {
    synchronized (blockerMap) {
      blockerMap.remove(name);
    }
  }

  @Override
  public Map<String, String> getBlockers() {
    synchronized (blockerMap) {
      Map<String, String> map = new HashMap<String, String>(blockerMap);
      return map;
    }
  }
}
