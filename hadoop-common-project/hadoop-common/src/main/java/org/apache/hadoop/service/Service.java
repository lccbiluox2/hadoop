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

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Service LifeCycle.
 *
 * 我们先说下Closeable接口。很简单，就是说实现该接口的类就表明有资源可以被关闭，关闭时调用
 * close()方法，但要注意处理IOException。他的爸即AutoCloseable的不同就是close方法抛出
 * 的是Exception异常，这里不再赘述。
 */
@Public
@Evolving
public interface Service extends Closeable {

  /**
   * Service states
   */
  // 服务状态枚举类
  public enum STATE {
    /** Constructed but not initialized */
    /** 已构建但尚未初始化 */
    NOTINITED(0, "NOTINITED"),

    /** Initialized but not started or stopped */
    /** 已初始化但还没有开始或结束 */
    INITED(1, "INITED"),

    /** started and not stopped */
    /** 已开始，尚未结束 */
    STARTED(2, "STARTED"),

    /** stopped. No further state transitions are permitted */
    /** 已结束，不允许再过度到其他状态 */
    STOPPED(3, "STOPPED");

    /**
     * An integer value for use in array lookup and JMX interfaces.
     * Although {@link Enum#ordinal()} could do this, explicitly
     * identify the numbers gives more stability guarantees over time.
     *
     * 一个int值，用来在数组查找和JXM接口。
     * 虽然Enamu的ordinal()方法有这个作用，但是随着时间推移提供更多的稳定性保证
     */
    private final int value;

    /**
     * A name of the state that can be used in messages
     */
    private final String statename;

    // todo: lcc  状态枚举类的构造方法，跟前文定义的状态序号和状态名匹配
    private STATE(int value, String name) {
      this.value = value;
      this.statename = name;
    }

    /**
     * Get the integer value of a state
     * @return the numeric value of the state
     */
    public int getValue() {
      return value;
    }

    /**
     * Get the name of a state
     * @return the state's name
     */
    @Override
    public String toString() {
      return statename;
    }
  }

  /**
   * Initialize the service.
   *
   * The transition MUST be from {@link STATE#NOTINITED} to {@link STATE#INITED}
   * unless the operation failed and an exception was raised, in which case
   * {@link #stop()} MUST be invoked and the service enter the state
   * {@link STATE#STOPPED}.
   * @param config the configuration of the service
   * @throws RuntimeException on any failure during the operation
   *
   *todo: 8/6/22 10:19 PM 九师兄
   * 服务初始化的方法
   *
   * 状态必须从 NOINITED -> INITED。
   * 除非init操作失败而且带有异常抛出时，在这种情况下stop方法必须被调用随后服务状态变为STOPPED
   *
   * config参数是关于服务的配置
   * 此外，要注意当操作过程中发生任何异常时会抛出RuntimeException
   *
   */
  void init(Configuration config);


  /**
   * Start the service.
   *
   * The transition MUST be from {@link STATE#INITED} to {@link STATE#STARTED}
   * unless the operation failed and an exception was raised, in which case
   * {@link #stop()} MUST be invoked and the service enter the state
   * {@link STATE#STOPPED}.
   * @throws RuntimeException on any failure during the operation
   */
  /**
   * 服务开始的方法
   *
   * 状态必须从 INITED -> STARTED。
   * 除非start操作失败而且带有异常抛出时，在这种情况下stop方法必须被调用随后服务状态变为STOPPED
   *
   * 要注意当操作过程中发生任何异常时会抛出RuntimeException
   */
  void start();

  /**
   * Stop the service. This MUST be a no-op if the service is already
   * in the {@link STATE#STOPPED} state. It SHOULD be a best-effort attempt
   * to stop all parts of the service.
   *
   * The implementation must be designed to complete regardless of the service
   * state, including the initialized/uninitialized state of all its internal
   * fields.
   * @throws RuntimeException on any failure during the stop operation
   */
  /**
   * 服务停止的方法
   *
   * 如果服务已经处于STOPPED状态时，则该操作必须是个空操作。
   * 该方法的实现中应该尽量关闭该服务的所有部分，不论服务处于什么状态都应该完成。
   *
   * 当操作过程中发生任何异常时会抛出RuntimeException
   */
  void stop();

  /**
   * A version of stop() that is designed to be usable in Java7 closure
   * clauses.
   * Implementation classes MUST relay this directly to {@link #stop()}
   * @throws IOException never
   * @throws RuntimeException on any failure during the stop operation
   */
  /**
   * 服务停止的方法
   * 设计为可在Java7闭包子句中使用的stop（）版本
   *
   * 永远不应该抛出IOException
   * 当操作过程中发生任何异常时会抛出RuntimeException
   */
  void close() throws IOException;

  /**
   * Register a listener to the service state change events.
   * If the supplied listener is already listening to this service,
   * this method is a no-op.
   * @param listener a new listener
   */
  // todo: 九师兄  注册服务状态更改事件的侦听器，如果已经注册过就为空操作
  void registerServiceListener(ServiceStateChangeListener listener);

  /**
   * Unregister a previously registered listener of the service state
   * change events. No-op if the listener is already unregistered.
   * @param listener the listener to unregister.
   */
  // todo: 5Lmd5biI5YWE  注销一个注册过的监听器，如果已经注销就为空操作
  void unregisterServiceListener(ServiceStateChangeListener listener);

  /**
   * Get the name of this service.
   * @return the service name
   */
  // 获取服务名
  String getName();

  /**
   * Get the configuration of this service.
   * This is normally not a clone and may be manipulated, though there are no
   * guarantees as to what the consequences of such actions may be
   * @return the current configuration, unless a specific implentation chooses
   * otherwise.
   */
  // 获取服务的配置
  Configuration getConfig();

  /**
   * Get the current service state
   * @return the state of the service
   */
  // 获取服务开始时间，若尚未开始就返回0
  STATE getServiceState();

  /**
   * Get the service start time
   * @return the start time of the service. This will be zero if the service
   * has not yet been started.
   */
  long getStartTime();

  /**
   * Query to see if the service is in a specific state.
   * In a multi-threaded system, the state may not hold for very long.
   * @param state the expected state
   * @return true if, at the time of invocation, the service was in that state.
   */
  // 判断服务是否处于传入的状态（判断结果仅限于调用时）
  boolean isInState(STATE state);

  /**
   * Get the first exception raised during the service failure. If null,
   * no exception was logged
   * @return the failure logged during a transition to the stopped state
   */
  // 获取服务中第一个抛出的异常，若没有异常记录就返回空
  Throwable getFailureCause();

  /**
   * Get the state in which the failure in {@link #getFailureCause()} occurred.
   * @return the state or null if there was no failure
   */
  //返回当getFailureCause()发生时的状态，如果没有发生过就返回空
  STATE getFailureState();

  /**
   * Block waiting for the service to stop; uses the termination notification
   * object to do so.
   *
   * This method will only return after all the service stop actions
   * have been executed (to success or failure), or the timeout elapsed
   * This method can be called before the service is inited or started; this is
   * to eliminate any race condition with the service stopping before
   * this event occurs.
   * @param timeout timeout in milliseconds. A value of zero means "forever"
   * @return true iff the service stopped in the time period
   */
  /**
   * 在指定时间内阻塞等待服务结束。
   *
   * 这个方法仅会在所有服务结束操作被执行完成后（得到成功或失败的结果）或者超出指定时间后才会返回。
   * 这个方法可以在服务INITED或是STARTED状态前调用，这样做是为了为了消除在此事件发生之前服务停止的任何竞争条件。
   *
   * timeout 参数为超时毫秒，0代表永远
   * 当服务在指定时间内停止时就返回true
   */
  boolean waitForServiceToStop(long timeout);

  /**
   * Get a snapshot of the lifecycle history; it is a static list
   * @return a possibly empty but never null list of lifecycle events.
   */
  // todo: 九师兄  返回状态转移的历史快照（静态list），如果没有记录就返回一个没有元素的非Null list
  public List<LifecycleEvent> getLifecycleHistory();

  /**
   * Get the blockers on a service -remote dependencies
   * that are stopping the service from being <i>live</i>.
   * @return a (snapshotted) map of blocker name-&gt;description values
   */
  public Map<String, String> getBlockers();
}
