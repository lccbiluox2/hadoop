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
package org.apache.hadoop.util;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.thirdparty.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.hadoop.util.concurrent.HadoopExecutors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.SERVICE_SHUTDOWN_TIMEOUT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.SERVICE_SHUTDOWN_TIMEOUT_DEFAULT;

/**
 * The <code>ShutdownHookManager</code> enables running shutdownHook
 * in a deterministic order, higher priority first.
 * <p>
 * The JVM runs ShutdownHooks in a non-deterministic order or in parallel.
 * This class registers a single JVM shutdownHook and run all the
 * shutdownHooks registered to it (to this class) in order based on their
 * priority.
 *
 * Unless a hook was registered with a shutdown explicitly set through
 * {@link #addShutdownHook(Runnable, int, long, TimeUnit)},
 * the shutdown time allocated to it is set by the configuration option
 * {@link CommonConfigurationKeysPublic#SERVICE_SHUTDOWN_TIMEOUT} in
 * {@code core-site.xml}, with a default value of
 * {@link CommonConfigurationKeysPublic#SERVICE_SHUTDOWN_TIMEOUT_DEFAULT}
 * seconds.
 *
 * 这段注释的意思大致是说ShutdownHookManager让注册进来的shutdownHook按顺序运行，并且高优先级先运行
 *  这样设计的原因是如果各自注册shutdownHook的话，JVM是没有办法保证执行顺序。
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public final class ShutdownHookManager {

  // todo: 九师兄 首先，包含一个饿汉式的ShutdownHookManager自身的单例对象：
  private static final ShutdownHookManager MGR = new ShutdownHookManager();

  private static final Logger LOG =
      LoggerFactory.getLogger(ShutdownHookManager.class);

  /** Minimum shutdown timeout: {@value} second(s). */
  public static final long TIMEOUT_MINIMUM = 1;

  /** The default time unit used: seconds. */
  public static final TimeUnit TIME_UNIT_DEFAULT = TimeUnit.SECONDS;

  private static final ExecutorService EXECUTOR =
      HadoopExecutors.newSingleThreadExecutor(new ThreadFactoryBuilder()
          .setDaemon(true)
          .setNameFormat("shutdown-hook-%01d")
          .build());

  /**
   *todo: 8/7/22 12:40 PM 九师兄
   *   //这里是向JVM注册了一个ShutdownHook，由一个线程来处理已经注册的ShutdownHook方法。
   *   //在执行shutdownHook处理前，会将原子类型的标志位shtdownInProgress设为true表名正在处理所有shutdownHook。
   *   //而且，这个过程是按注册时的优先级来排序的。
   **/
  static {
    try {
      Runtime.getRuntime().addShutdownHook(
        new Thread() {
          @Override
          public void run() {
            if (MGR.shutdownInProgress.getAndSet(true)) {
              LOG.info("Shutdown process invoked a second time: ignoring");
              return;
            }
            long started = System.currentTimeMillis();
            int timeoutCount = MGR.executeShutdown();
            long ended = System.currentTimeMillis();
            LOG.debug(String.format(
                "Completed shutdown in %.3f seconds; Timeouts: %d",
                (ended-started)/1000.0, timeoutCount));
            // each of the hooks have executed; now shut down the
            // executor itself.
            shutdownExecutor(new Configuration());
          }
        }
      );
    } catch (IllegalStateException ex) {
      // JVM is being shut down. Ignore
      LOG.warn("Failed to add the ShutdownHook", ex);
    }
  }

  /**
   * Execute the shutdown.
   * This is exposed purely for testing: do not invoke it.
   * @return the number of shutdown hooks which timed out.
   */
  @InterfaceAudience.Private
  @VisibleForTesting
  int executeShutdown() {
    int timeouts = 0;
    for (HookEntry entry: getShutdownHooksInOrder()) {
      Future<?> future = EXECUTOR.submit(entry.getHook());
      try {
        future.get(entry.getTimeout(), entry.getTimeUnit());
      } catch (TimeoutException ex) {
        timeouts++;
        future.cancel(true);
        LOG.warn("ShutdownHook '" + entry.getHook().getClass().
            getSimpleName() + "' timeout, " + ex.toString(), ex);
      } catch (Throwable ex) {
        LOG.warn("ShutdownHook '" + entry.getHook().getClass().
            getSimpleName() + "' failed, " + ex.toString(), ex);
      }
    }
    return timeouts;
  }

  /**
   * Shutdown the executor thread itself.
   * @param conf the configuration containing the shutdown timeout setting.
   */
  private static void shutdownExecutor(final Configuration conf) {
    try {
      EXECUTOR.shutdown();
      long shutdownTimeout = getShutdownTimeout(conf);
      if (!EXECUTOR.awaitTermination(
          shutdownTimeout,
          TIME_UNIT_DEFAULT)) {
        // timeout waiting for the
        LOG.error("ShutdownHookManager shutdown forcefully after"
            + " {} seconds.", shutdownTimeout);
        EXECUTOR.shutdownNow();
      }
      LOG.debug("ShutdownHookManager completed shutdown.");
    } catch (InterruptedException ex) {
      // interrupted.
      LOG.error("ShutdownHookManager interrupted while waiting for " +
          "termination.", ex);
      EXECUTOR.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Return <code>ShutdownHookManager</code> singleton.
   *
   * @return <code>ShutdownHookManager</code> singleton.
   */
  @InterfaceAudience.Public
  public static ShutdownHookManager get() {
    return MGR;
  }

  /**
   * Get the shutdown timeout in seconds, from the supplied
   * configuration.
   * @param conf configuration to use.
   * @return a timeout, always greater than or equal to {@link #TIMEOUT_MINIMUM}
   */
  @InterfaceAudience.Private
  @VisibleForTesting
  static long getShutdownTimeout(Configuration conf) {
    long duration = conf.getTimeDuration(
        SERVICE_SHUTDOWN_TIMEOUT,
        SERVICE_SHUTDOWN_TIMEOUT_DEFAULT,
        TIME_UNIT_DEFAULT);
    if (duration < TIMEOUT_MINIMUM) {
      duration = TIMEOUT_MINIMUM;
    }
    return duration;
  }

  /**
   * Private structure to store ShutdownHook, its priority and timeout
   * settings.
   *
   * HookEntry的代码很简单，就是封装了ShutdownHook线程对象和优先级的一个类。
   *
   * 重写了equals方法，判断的标准是内部的名为hook的Runnable对象是否相同。
   */
  @InterfaceAudience.Private
  @VisibleForTesting
  static class HookEntry {
    private final Runnable hook;
    private final int priority;
    private final long timeout;
    private final TimeUnit unit;

    HookEntry(Runnable hook, int priority) {
      this(hook, priority,
          getShutdownTimeout(new Configuration()),
          TIME_UNIT_DEFAULT);
    }

    HookEntry(Runnable hook, int priority, long timeout, TimeUnit unit) {
      this.hook = hook;
      this.priority = priority;
      this.timeout = timeout;
      this.unit = unit;
    }

    @Override
    public int hashCode() {
      return hook.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      boolean eq = false;
      if (obj != null) {
        if (obj instanceof HookEntry) {
          eq = (hook == ((HookEntry)obj).hook);
        }
      }
      return eq;
    }

    Runnable getHook() {
      return hook;
    }

    int getPriority() {
      return priority;
    }

    long getTimeout() {
      return timeout;
    }

    TimeUnit getTimeUnit() {
      return unit;
    }
  }

  //定义了一个存放shutdownHook的是线程安全的HashSet，以HookEntry来进行了封装了ShutdownHook对象。
  private final Set<HookEntry> hooks =
      Collections.synchronizedSet(new HashSet<HookEntry>());

  //用原子类型的布尔值表示shutdownHook是否正常处理中
  private AtomicBoolean shutdownInProgress = new AtomicBoolean(false);

  //private to constructor to ensure singularity
  @VisibleForTesting
  @InterfaceAudience.Private
  ShutdownHookManager() {
  }

  /**
   * Returns the list of shutdownHooks in order of execution,
   * Highest priority first.
   *
   * @return the list of shutdownHooks in order of execution.
   *
   * 按执行顺序来返回shutdownHook列表，高优先级在前
   */
  @InterfaceAudience.Private
  @VisibleForTesting
  List<HookEntry> getShutdownHooksInOrder() {
    List<HookEntry> list;
    //这里就是获取了装有所有hooks的set对象的对象锁，转为list
    synchronized (hooks) {
      list = new ArrayList<>(hooks);
    }

    //对hook list进行排序，高优先级排在前
    Collections.sort(list, new Comparator<HookEntry>() {

      //reversing comparison so highest priority hooks are first
      @Override
      public int compare(HookEntry o1, HookEntry o2) {
        return o2.priority - o1.priority;
      }
    });
    //最终返回这个已排好序的shutdownHook runnable list
    return list;
  }

  /**
   * Adds a shutdownHook with a priority, the higher the priority
   * the earlier will run. ShutdownHooks with same priority run
   * in a non-deterministic order.
   *
   * @param shutdownHook shutdownHook <code>Runnable</code>
   * @param priority priority of the shutdownHook.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public void addShutdownHook(Runnable shutdownHook, int priority) {
    if (shutdownHook == null) {
      throw new IllegalArgumentException("shutdownHook cannot be NULL");
    }
    if (shutdownInProgress.get()) {
      throw new IllegalStateException("Shutdown in progress, cannot add a " +
          "shutdownHook");
    }
    hooks.add(new HookEntry(shutdownHook, priority));
  }

  /**
   *
   * Adds a shutdownHook with a priority and timeout the higher the priority
   * the earlier will run. ShutdownHooks with same priority run
   * in a non-deterministic order. The shutdown hook will be terminated if it
   * has not been finished in the specified period of time.
   *
   * @param shutdownHook shutdownHook <code>Runnable</code>
   * @param priority priority of the shutdownHook
   * @param timeout timeout of the shutdownHook
   * @param unit unit of the timeout <code>TimeUnit</code>
   */
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public void addShutdownHook(Runnable shutdownHook, int priority, long timeout,
      TimeUnit unit) {
    if (shutdownHook == null) {
      throw new IllegalArgumentException("shutdownHook cannot be NULL");
    }
    if (shutdownInProgress.get()) {
      throw new IllegalStateException("Shutdown in progress, cannot add a " +
          "shutdownHook");
    }
    hooks.add(new HookEntry(shutdownHook, priority, timeout, unit));
  }

  /**
   * Removes a shutdownHook.
   *
   * @param shutdownHook shutdownHook to remove.
   * @return TRUE if the shutdownHook was registered and removed,
   * FALSE otherwise.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public boolean removeShutdownHook(Runnable shutdownHook) {
    if (shutdownInProgress.get()) {
      throw new IllegalStateException("Shutdown in progress, cannot remove a " +
          "shutdownHook");
    }
    // hooks are only == by runnable
    return hooks.remove(new HookEntry(shutdownHook, 0, TIMEOUT_MINIMUM,
      TIME_UNIT_DEFAULT));
  }

  /**
   * Indicates if a shutdownHook is registered or not.
   *
   * @param shutdownHook shutdownHook to check if registered.
   * @return TRUE/FALSE depending if the shutdownHook is is registered.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public boolean hasShutdownHook(Runnable shutdownHook) {
    return hooks.contains(new HookEntry(shutdownHook, 0, TIMEOUT_MINIMUM,
      TIME_UNIT_DEFAULT));
  }
  
  /**
   * Indicates if shutdown is in progress or not.
   * 
   * @return TRUE if the shutdown is in progress, otherwise FALSE.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public boolean isShutdownInProgress() {
    return shutdownInProgress.get();
  }

  /**
   * clear all registered shutdownHooks.
   */
  @InterfaceAudience.Public
  @InterfaceStability.Stable
  public void clearShutdownHooks() {
    hooks.clear();
  }
}
