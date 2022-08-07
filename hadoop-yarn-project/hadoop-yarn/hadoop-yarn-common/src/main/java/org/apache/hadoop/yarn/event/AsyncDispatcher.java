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

package org.apache.hadoop.yarn.event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.hadoop.yarn.metrics.EventTypeMetrics;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.MonotonicClock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import org.apache.hadoop.thirdparty.com.google.common.annotations.VisibleForTesting;

/**
 * Dispatches {@link Event}s in a separate thread. Currently only single thread
 * does that. Potentially there could be multiple channels for each event type
 * class and a thread pool can be used to dispatch the events.
 *
 * 在一个单独的线程中分派{@link事件}。目前只有一个线程可以这样做。
 * 每个事件类型类可能有多个通道，可以使用线程池来分派事件。
 *
 * 用单独的线程分发处理多个事件。当前版本只有一个单独的类，
 * 但是潜在的可以对每个事件类型类有一个channel，可以用一个线程池来处理多个事件
 */
@SuppressWarnings("rawtypes")
@Public
@Evolving
public class AsyncDispatcher extends AbstractService implements Dispatcher {

  private static final Logger LOG =
      LoggerFactory.getLogger(AsyncDispatcher.class);
  private static final Marker FATAL =
      MarkerFactory.getMarker("FATAL");

  // 九师兄 事件队列，存放封装了各类事件的Event对象的阻塞队列
  private final BlockingQueue<Event> eventQueue;
  private volatile int lastEventQueueSizeLogged = 0;
  private volatile int lastEventDetailsQueueSizeLogged = 0;
  // todo: 九师兄  标记是否dispatcher应该结束
  private volatile boolean stopped = false;

  //Configuration for control the details queue event printing.
  private int detailsInterval;
  private boolean printTrigger = false;

  // Configuration flag for enabling/disabling draining dispatcher's events on
  // stop functionality.
  // todo: 九师兄  配置标志，用于启用/禁用在停止功能上排空调度程序的事件。
  private volatile boolean drainEventsOnStop = false;

  // Indicates all the remaining dispatcher's events on stop have been drained
  // and processed.
  // Race condition happens if dispatcher thread sets drained to true between
  // handler setting drained to false and enqueueing event. YARN-3878 decided
  // to ignore it because of its tiny impact. Also see YARN-5436.
  /**
   *todo: 8/7/22 11:25 AM 九师兄
   * 表示所有剩余的已停止调度事件已经被处理
   * 指示所有在停止时剩余的调度程序事件都已清除并处理。如果调度程序线程在处理程序设置为false
   * 和排队事件之间将drain设置为true，则会发生竞态条件。YARN-3878决定忽略它，因为它的影响很小。
   *  YARN-5436。
   */
  private volatile boolean drained = true;
  // 等待排空的对象（锁）
  private final Object waitForDrained = new Object();

  // For drainEventsOnStop enabled only, block newly coming events into the
  // queue while stopping.
  // 仅当drainEventsOnStop为true时可用，如果为true表示在停止时会阻塞新来的事件加入队列
  private volatile boolean blockNewEvents = false;
  /**
   *todo: 8/7/22 11:27 AM 九师兄
   *   GenericEventHandler负责往eventQueue里放入event
   **/
  private final EventHandler<Event> handlerInstance = new GenericEventHandler();

  // todo: 九师兄  处理所有事件的线程
  private Thread eventHandlingThread;
  /*
   * 九师兄 jiushixiong lcc
   * 未经允许，不得转载。主页：https://blog.csdn.net/qq_21383435
   *
   *  Map<事件类型, 能处理这类事件的EventHandler>
   *  存放不同事件对应的事件处理器的映射表
   **/
  protected final Map<Class<? extends Enum>, EventHandler> eventDispatchers;
  private boolean exitOnDispatchException = true;

  private Map<Class<? extends Enum>,
      EventTypeMetrics> eventTypeMetricsMap;

  private Clock clock = new MonotonicClock();

  /**
   * The thread name for dispatcher.
   */
  private String dispatcherThreadName = "AsyncDispatcher event handler";

  public AsyncDispatcher() {
    // todo: 初始化了一个队列
    this(new LinkedBlockingQueue<Event>());
  }

  public AsyncDispatcher(BlockingQueue<Event> eventQueue) {
    // 这里就是用的AbstractService的构造方法，创建一个名为Dispatcher的服务，初始状态为UNINITED
    super("Dispatcher");
    this.eventQueue = eventQueue;
    this.eventDispatchers = new HashMap<Class<? extends Enum>, EventHandler>();
    this.eventTypeMetricsMap = new HashMap<Class<? extends Enum>,
        EventTypeMetrics>();
  }

  /**
   * Set a name for this dispatcher thread.
   * @param dispatcherName name of the dispatcher thread
   */
  public AsyncDispatcher(String dispatcherName) {
    this();
    dispatcherThreadName = dispatcherName;
  }

  /**
   *todo: 8/7/22 11:30 AM 九师兄
   * 这就是处理所有事件的线程了
   **/
  Runnable createThread() {
    return new Runnable() {
      @Override
      public void run() {
        // 如果没有停止，而且没有被中断，那么就死循环
        while (!stopped && !Thread.currentThread().isInterrupted()) {
          drained = eventQueue.isEmpty();
          // blockNewEvents is only set when dispatcher is draining to stop,
          // adding this check is to avoid the overhead of acquiring the lock
          // and calling notify every time in the normal run of the loop.
          //
          // blockNewEvents只在调度程序耗尽到停止时设置，添加这个检查是为了避免
          // 在每次循环正常运行时获取锁和调用notify的开销。
          // blockNewEvents仅在dispatcher排空、停止时设置为true，
          // 所以此检查是为了避免每次在循环的正常运行中获取锁和调用notify的开销。
          if (blockNewEvents) {
            synchronized (waitForDrained) {
              if (drained) {
                waitForDrained.notify();
              }
            }
          }
          Event event;
          try {
            // todo: 九师兄  从事件队列中拿出来一个事件
            event = eventQueue.take();
          } catch(InterruptedException ie) {
            if (!stopped) {
              LOG.warn("AsyncDispatcher thread interrupted", ie);
            }
            return;
          }
          if (event != null) {
            if (eventTypeMetricsMap.
                get(event.getType().getDeclaringClass()) != null) {
              long startTime = clock.getTime();
              // todo: 分发处理事件
              dispatch(event);
              eventTypeMetricsMap.get(event.getType().getDeclaringClass())
                  .increment(event.getType(),
                      clock.getTime() - startTime);
            } else {
              // todo: 分发处理事件
              dispatch(event);
            }
            if (printTrigger) {
              //Log the latest dispatch event type
              // may cause the too many events queued
              LOG.info("Latest dispatch event type: " + event.getType());
              printTrigger = false;
            }
          }
        }
      }
    };
  }

  @VisibleForTesting
  public void disableExitOnDispatchException() {
    exitOnDispatchException = false;
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception{
    super.serviceInit(conf);
    this.detailsInterval = getConfig().getInt(YarnConfiguration.
                    YARN_DISPATCHER_PRINT_EVENTS_INFO_THRESHOLD,
            YarnConfiguration.
                    DEFAULT_YARN_DISPATCHER_PRINT_EVENTS_INFO_THRESHOLD);
  }

  @Override
  protected void serviceStart() throws Exception {
    //start all the components
    super.serviceStart();
    // 创建一个线程 并且启动
    eventHandlingThread = new Thread(createThread());
    eventHandlingThread.setName(dispatcherThreadName);
    eventHandlingThread.start();
  }

  public void setDrainEventsOnStop() {
    drainEventsOnStop = true;
  }

  @Override
  protected void serviceStop() throws Exception {
    if (drainEventsOnStop) {
      // 注意，这里就设置了 blockNewEvents = true，上面的线程会用
      blockNewEvents = true;
      LOG.info("AsyncDispatcher is draining to stop, ignoring any new events.");
      long endTime = System.currentTimeMillis() + getConfig()
          .getLong(YarnConfiguration.DISPATCHER_DRAIN_EVENTS_TIMEOUT,
              YarnConfiguration.DEFAULT_DISPATCHER_DRAIN_EVENTS_TIMEOUT);

      synchronized (waitForDrained) {
        // todo: 九师兄  等待服务处理排空，而且eventHandling线程还活着
        while (!isDrained() && eventHandlingThread != null
            && eventHandlingThread.isAlive()
            && System.currentTimeMillis() < endTime) {
          waitForDrained.wait(100);
          LOG.info("Waiting for AsyncDispatcher to drain. Thread state is :" +
              eventHandlingThread.getState());
        }
      }
    }
    stopped = true;
    // todo: 九师兄 结束处理剩余事件后就将eventHandlingThread interrupt停止
    if (eventHandlingThread != null) {
      eventHandlingThread.interrupt();
      try {
        eventHandlingThread.join();
      } catch (InterruptedException ie) {
        LOG.warn("Interrupted Exception while stopping", ie);
      }
    }

    // stop all the components
    super.serviceStop();
  }

  /**
   *todo: 8/7/22 11:30 AM 九师兄
   * 处理事件的方法
   **/
  @SuppressWarnings("unchecked")
  protected void dispatch(Event event) {
    //all events go thru this loop
    LOG.debug("Dispatching the event {}.{}", event.getClass().getName(),
        event);

    // 九师兄 todo: 找到事件的类型
    // 获取该事件类型的枚举class
    // 可以看到这里用了继承Enum的泛型，因为所有的event type都被设计为泛型类
    Class<? extends Enum> type = event.getType().getDeclaringClass();

    try{
      // 九师兄 todo: 根据事件的类 获取相应的EventHandler
      EventHandler handler = eventDispatchers.get(type);
      if(handler != null) {
        // 九师兄 todo: 调用响应事件的EventHandler处理事件
        handler.handle(event);
      } else {
        throw new Exception("No handler for registered for " + type);
      }
    } catch (Throwable t) {
      //TODO Maybe log the state of the queue
      LOG.error(FATAL, "Error in dispatcher thread", t);
      // If serviceStop is called, we should exit this thread gracefully.
      // todo: 九师兄  当满足以下条件时才需要直接退出
      if (exitOnDispatchException
          && (ShutdownHookManager.get().isShutdownInProgress()) == false
          && stopped == false) {
        stopped = true;
        Thread shutDownThread = new Thread(createShutDownThread());
        shutDownThread.setName("AsyncDispatcher ShutDown handler");
        shutDownThread.start();
      }
    }
  }

  /**
   *todo: 8/7/22 11:32 AM 九师兄
   * 这个就是向AsyncDispatcher注册事件类型和对应handler的方法
   **/
  @SuppressWarnings("unchecked")
  @Override
  public void register(Class<? extends Enum> eventType,
      EventHandler handler) {
    /* check to see if we have a listener registered */
    // todo: 九师兄  先检查是不是已存在该类型事件的handler
    EventHandler<Event> registeredHandler = (EventHandler<Event>)
    eventDispatchers.get(eventType);
    LOG.info("Registering " + eventType + " for " + handler.getClass());
    if (registeredHandler == null) {
      // 九师兄 todo: 放入事件类型和处理器EventHandler的映射关系
      eventDispatchers.put(eventType, handler);
    } else if (!(registeredHandler instanceof MultiListenerHandler)){
      /* for multiple listeners of an event add the multiple listener handler */
      // todo: 九师兄  如果不是MultiListenerHandler类型，就将老的取出和参数handler一起加到新建的multiHandler
      MultiListenerHandler multiHandler = new MultiListenerHandler();
      multiHandler.addHandler(registeredHandler);
      multiHandler.addHandler(handler);
      // 九师兄 todo: 放入事件类型和处理器EventHandler的映射关系
      eventDispatchers.put(eventType, multiHandler);
    } else {
      /* already a multilistener, just add to it */
      // todo: 九师兄  已经是MultiListenerHandler，直接添加
      MultiListenerHandler multiHandler
      = (MultiListenerHandler) registeredHandler;
      multiHandler.addHandler(handler);
    }
  }

  @Override
  public EventHandler<Event> getEventHandler() {
    return handlerInstance;
  }

  /**
   *todo: 8/7/22 11:29 AM 九师兄
   *  GenericEventHandler是生产者
   *  一般的事件处理套路是某个对象调用此君的handle方法，放入eventQueue
   *  然后createThread线程取出事件，调用dispatch方法
   *  dispatch方法内会找到事件type对应的handler并调用其handle方法处理该事件
   **/
  class GenericEventHandler implements EventHandler<Event> {
    private void printEventQueueDetails() {
      Iterator<Event> iterator = eventQueue.iterator();
      Map<Enum, Long> counterMap = new HashMap<>();
      while (iterator.hasNext()) {
        Enum eventType = iterator.next().getType();
        if (!counterMap.containsKey(eventType)) {
          counterMap.put(eventType, 0L);
        }
        counterMap.put(eventType, counterMap.get(eventType) + 1);
      }
      for (Map.Entry<Enum, Long> entry : counterMap.entrySet()) {
        long num = entry.getValue();
        LOG.info("Event type: " + entry.getKey()
                + ", Event record counter: " + num);
      }
    }
    public void handle(Event event) {
      if (blockNewEvents) {
        return;
      }
      drained = false;

      /* all this method does is enqueue all the events onto the queue */
      int qSize = eventQueue.size();
      if (qSize != 0 && qSize % 1000 == 0
          && lastEventQueueSizeLogged != qSize) {
        lastEventQueueSizeLogged = qSize;
        LOG.info("Size of event-queue is " + qSize);
      }
      if (qSize != 0 && qSize % detailsInterval == 0
              && lastEventDetailsQueueSizeLogged != qSize) {
        lastEventDetailsQueueSizeLogged = qSize;
        printEventQueueDetails();
        printTrigger = true;
      }
      int remCapacity = eventQueue.remainingCapacity();
      if (remCapacity < 1000) {
        LOG.warn("Very low remaining capacity in the event-queue: "
            + remCapacity);
      }
      try {
        // 九师兄 todo: 放入事件,这里就把他放入事件阻塞队列了
        eventQueue.put(event);
      } catch (InterruptedException e) {
        if (!stopped) {
          LOG.warn("AsyncDispatcher thread interrupted", e);
        }
        // Need to reset drained flag to true if event queue is empty,
        // otherwise dispatcher will hang on stop.
        // todo: 九师兄  如果队列已经排空，就设drained为true，否则wait在waitForDrained上的线程会一直挂起
        drained = eventQueue.isEmpty();
        throw new YarnRuntimeException(e);
      }
    };
  }

  /**
   * Multiplexing an event. Sending it to different handlers that
   * are interested in the event.
   * @param <T> the type of event these multiple handlers are interested in.
   */
  static class MultiListenerHandler implements EventHandler<Event> {
    List<EventHandler<Event>> listofHandlers;

    public MultiListenerHandler() {
      listofHandlers = new ArrayList<EventHandler<Event>>();
    }

    @Override
    public void handle(Event event) {
      for (EventHandler<Event> handler: listofHandlers) {
        handler.handle(event);
      }
    }

    void addHandler(EventHandler<Event> handler) {
      listofHandlers.add(handler);
    }

  }

  /**
   *todo: 8/7/22 11:32 AM 九师兄
   * 这个就是上面异常时用到的退出线程
   **/
  Runnable createShutDownThread() {
    return new Runnable() {
      @Override
      public void run() {
        LOG.info("Exiting, bbye..");
        System.exit(-1);
      }
    };
  }

  @VisibleForTesting
  protected boolean isEventThreadWaiting() {
    return eventHandlingThread.getState() == Thread.State.WAITING;
  }

  protected boolean isDrained() {
    return drained;
  }

  protected boolean isStopped() {
    return stopped;
  }

  public void addMetrics(EventTypeMetrics metrics,
      Class<? extends Enum> eventClass) {
    eventTypeMetricsMap.put(eventClass, metrics);
  }
}
