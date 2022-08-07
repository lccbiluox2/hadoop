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

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Composition of services.
 */
@Public
@Evolving
public class CompositeService extends AbstractService {

  private static final Logger LOG =
      LoggerFactory.getLogger(CompositeService.class);

  /**
   * Policy on shutdown: attempt to close everything (purest) or
   * only try to close started services (which assumes
   * that the service implementations may not handle the stop() operation
   * except when started.
   * Irrespective of this policy, if a child service fails during
   * its init() or start() operations, it will have stop() called on it.
   */
  // todo: 九师兄 下面这个常量表示总是关闭所有，包括STARTED、INITED状态的服务。
  protected static final boolean STOP_ONLY_STARTED_SERVICES = false;

  /**
   * 非常重要的一个类，存储service列表,存各子服务的list，前面提到过这个类是组合了多个服务
   */
  private final List<Service> serviceList = new ArrayList<Service>();

  //构造方法，传入服务名
  public CompositeService(String name) {
    super(name);
  }

  /**
   * Get a cloned list of services
   * @return a list of child services at the time of invocation -
   * added services will not be picked up.
   */
  // todo: 九师兄 获取子服务列表，注意这里使用了serviceList对象作为对象锁，
  //  也就是说获取的时候如果刚好有service添加将被阻塞不能被获取到
  public List<Service> getServices() {
    synchronized (serviceList) {
      return new ArrayList<Service>(serviceList);
    }
  }

  /**
   * Add the passed {@link Service} to the list of services managed by this
   * {@link CompositeService}
   * @param service the {@link Service} to be added
   *                //添加服务到子服务列表
   */
  protected void addService(Service service) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Adding service " + service.getName());
    }
    synchronized (serviceList) {
      serviceList.add(service);
    }
  }

  /**
   * If the passed object is an instance of {@link Service},
   * add it to the list of services managed by this {@link CompositeService}
   * @param object
   * @return true if a service is added, false otherwise.
   */
  // todo: 九师兄 添加服务，在此之前判断是否是Service实现类
  protected boolean addIfService(Object object) {
    if (object instanceof Service) {
      addService((Service) object);
      return true;
    } else {
      return false;
    }
  }

  protected boolean removeService(Service service) {
    LOG.debug("Removing service {}", service.getName());
    synchronized (serviceList) {
      return serviceList.remove(service);
    }
  }

  /**
   *todo: 8/7/22 10:06 AM 九师兄
   * //服务启动方法，很简单，获取本对象中存放的服务的list，挨个调用其init方法启动
   **/
  protected void serviceInit(Configuration conf) throws Exception {
    // 获取所有的seriver
    List<Service> services = getServices();
    if (LOG.isDebugEnabled()) {
      LOG.debug(getName() + ": initing services, size=" + services.size());
    }
    // 循环调用每个service的init方法 调用的是抽象类 AbstractService的init方法
    for (Service service : services) {
      service.init(conf);
    }
    // 调研父类的serviceInit
    super.serviceInit(conf);
  }

  protected void serviceStart() throws Exception {
    // 获取所有的子服务
    List<Service> services = getServices();
    if (LOG.isDebugEnabled()) {
      LOG.debug(getName() + ": starting services, size=" + services.size());
    }
    for (Service service : services) {
      // start the service. If this fails that service
      // will be stopped and an exception raised
      // 循环调用start方法
      service.start();
    }
    super.serviceStart();
  }

  protected void serviceStop() throws Exception {
    //stop all services that were started
    int numOfServicesToStop = serviceList.size();
    if (LOG.isDebugEnabled()) {
      LOG.debug(getName() + ": stopping services, size=" + numOfServicesToStop);
    }
    stop(numOfServicesToStop, STOP_ONLY_STARTED_SERVICES);
    super.serviceStop();
  }

  /**
   * Stop the services in reverse order
   *
   * @param numOfServicesStarted index from where the stop should work
   * @param stopOnlyStartedServices flag to say "only start services that are
   * started, not those that are NOTINITED or INITED.
   * @throws RuntimeException the first exception raised during the
   * stop process -<i>after all services are stopped</i>
   */
  private void stop(int numOfServicesStarted, boolean stopOnlyStartedServices) {
    // stop in reverse order of start
    Exception firstException = null;
    List<Service> services = getServices();
    for (int i = numOfServicesStarted - 1; i >= 0; i--) {
      Service service = services.get(i);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Stopping service #" + i + ": " + service);
      }
      STATE state = service.getServiceState();
      //depending on the stop police
      if (state == STATE.STARTED 
         || (!stopOnlyStartedServices && state == STATE.INITED)) {
        Exception ex = ServiceOperations.stopQuietly(LOG, service);
        if (ex != null && firstException == null) {
          firstException = ex;
        }
      }
    }
    //after stopping all services, rethrow the first exception raised
    if (firstException != null) {
      throw ServiceStateException.convert(firstException);
    }
  }

  /**
   * JVM Shutdown hook for CompositeService which will stop the give
   * CompositeService gracefully in case of JVM shutdown.
   */
  public static class CompositeServiceShutdownHook implements Runnable {

    private CompositeService compositeService;

    public CompositeServiceShutdownHook(CompositeService compositeService) {
      this.compositeService = compositeService;
    }

    @Override
    public void run() {
      ServiceOperations.stopQuietly(compositeService);
    }
  }

}
