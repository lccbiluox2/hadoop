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

package org.apache.hadoop.yarn.server.nodemanager.health;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.util.Shell.ExitCodeException;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The class which provides functionality of checking the health of the node
 * using the configured node health script and reporting back to the service
 * for which the health checker has been asked to report.
 *
 * todo: 健康检查
 * 健康检查也可以用于动态升级。利用健康检查脚本机制告诉RM不在分配任务，等NM升级好了再分配。
 * 两种策略判断健康情况：
 *   一、自定义Shell，一旦脚本输出ERROR开头的字符串，则认为节点不健康。
 *   二、判断磁盘好坏，NM上有周期性检测磁盘模块，如果坏磁盘数达到一定比例则认为不健康。
 *
 * NodeHealthScriptRunner
 * 周期性执行脚本发现标准输出有以ERROR开头的输出，则通过心跳告诉RM，RM将其拉黑，此后不会分配新容器，
 * 老容器继续运行。只要NM活着会一直运行该脚本，当不输出ERROR则通过心跳向RM汇报，移除拉黑。
 *
 * 实践：由于Yarn只对CPU和内存隔离。网络和磁盘IO不隔离。不同任务会互相干扰。健康检查脚本可以缓解此问题。
 * 脚本去检查网络、磁盘、文件系统等情况。发现网络拥塞、磁盘空间不足、文件系统故障，可将状态变为不健康。
 *
 * LocalDirsHandlerService
 * 一旦发现正常比例低于配置值则设为不健康状态。判断磁盘好坏方式是目录具有读、写、执行三权限。判断
 * local-dirs和log-dirs所设置的目录。
 */
public class NodeHealthScriptRunner extends TimedHealthReporterService {

  private static final Logger LOG =
      LoggerFactory.getLogger(NodeHealthScriptRunner.class);

  /** Absolute path to the health script. */
  private String nodeHealthScript;
  /** Time after which the script should be timed out. */
  private long scriptTimeout;
  /** ShellCommandExecutor used to execute monitoring script. */
  private ShellCommandExecutor commandExecutor = null;

  /** Pattern used for searching in the output of the node health script. */
  private static final String ERROR_PATTERN = "ERROR";

  /** Time out error message. */
  static final String NODE_HEALTH_SCRIPT_TIMED_OUT_MSG =
      "Node health script timed out";

  private NodeHealthScriptRunner(String scriptName, long checkInterval,
      long timeout, String[] scriptArgs, boolean runBeforeStartup) {
    super(NodeHealthScriptRunner.class.getName(), checkInterval,
        runBeforeStartup);
    this.nodeHealthScript = scriptName;
    this.scriptTimeout = timeout;
    setTimerTask(new NodeHealthMonitorExecutor(scriptArgs));
  }

  public static NodeHealthScriptRunner newInstance(String scriptName,
      Configuration conf) {
    String nodeHealthScriptsConfig = String.format(
        YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_PATH_TEMPLATE, scriptName);
    String nodeHealthScript = conf.get(nodeHealthScriptsConfig);
    if (!shouldRun(scriptName, nodeHealthScript)) {
      return null;
    }

    // Determine check interval ms
    String checkIntervalMsConfig = String.format(
        YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_INTERVAL_MS_TEMPLATE,
        scriptName);
    long checkIntervalMs = conf.getLong(checkIntervalMsConfig, 0L);
    if (checkIntervalMs == 0L) {
      checkIntervalMs = conf.getLong(
          YarnConfiguration.NM_HEALTH_CHECK_INTERVAL_MS,
          YarnConfiguration.DEFAULT_NM_HEALTH_CHECK_INTERVAL_MS);
    }
    if (checkIntervalMs < 0) {
      throw new IllegalArgumentException("The node health-checker's " +
          "interval-ms can not be set to a negative number.");
    }

    boolean runBeforeStartup = conf.getBoolean(
        YarnConfiguration.NM_HEALTH_CHECK_RUN_BEFORE_STARTUP,
        YarnConfiguration.DEFAULT_NM_HEALTH_CHECK_RUN_BEFORE_STARTUP);

    // Determine time out
    String scriptTimeoutConfig = String.format(
        YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_TIMEOUT_MS_TEMPLATE,
        scriptName);
    long scriptTimeout = conf.getLong(scriptTimeoutConfig, 0L);
    if (scriptTimeout == 0L) {
      scriptTimeout = conf.getLong(
          YarnConfiguration.NM_HEALTH_CHECK_TIMEOUT_MS,
          YarnConfiguration.DEFAULT_NM_HEALTH_CHECK_TIMEOUT_MS);
    }
    if (scriptTimeout <= 0) {
      throw new IllegalArgumentException("The node health-checker's " +
          "timeout can only be set to a positive number.");
    }

    // Determine script arguments
    String scriptArgsConfig = String.format(
        YarnConfiguration.NM_HEALTH_CHECK_SCRIPT_OPTS_TEMPLATE,
        scriptName);
    String[] scriptArgs = conf.getStrings(scriptArgsConfig, new String[]{});

    return new NodeHealthScriptRunner(nodeHealthScript,
        checkIntervalMs, scriptTimeout, scriptArgs, runBeforeStartup);
  }

  private enum HealthCheckerExitStatus {
    SUCCESS,
    TIMED_OUT,
    FAILED_WITH_EXIT_CODE,
    FAILED_WITH_EXCEPTION,
    FAILED
  }


  /**
   * Class which is used by the {@link Timer} class to periodically execute the
   * node health script.
   */
  private class NodeHealthMonitorExecutor extends TimerTask {
    private String exceptionStackTrace = "";

    NodeHealthMonitorExecutor(String[] args) {
      ArrayList<String> execScript = new ArrayList<String>();
      execScript.add(nodeHealthScript);
      if (args != null) {
        execScript.addAll(Arrays.asList(args));
      }
      commandExecutor = new ShellCommandExecutor(execScript
          .toArray(new String[execScript.size()]), null, null, scriptTimeout);
    }

    @Override
    public void run() {
      HealthCheckerExitStatus status = HealthCheckerExitStatus.SUCCESS;
      try {
        commandExecutor.execute();
      } catch (ExitCodeException e) {
        // ignore the exit code of the script
        status = HealthCheckerExitStatus.FAILED_WITH_EXIT_CODE;
        // On Windows, we will not hit the Stream closed IOException
        // thrown by stdout buffered reader for timeout event.
        if (Shell.WINDOWS && commandExecutor.isTimedOut()) {
          status = HealthCheckerExitStatus.TIMED_OUT;
        }
      } catch (Exception e) {
        LOG.warn("Caught exception : " + e.getMessage());
        if (!commandExecutor.isTimedOut()) {
          status = HealthCheckerExitStatus.FAILED_WITH_EXCEPTION;
        } else {
          status = HealthCheckerExitStatus.TIMED_OUT;
        }
        exceptionStackTrace = StringUtils.stringifyException(e);
      } finally {
        if (status == HealthCheckerExitStatus.SUCCESS) {
          if (hasErrors(commandExecutor.getOutput())) {
            status = HealthCheckerExitStatus.FAILED;
          }
        }
        reportHealthStatus(status);
      }
    }

    /**
     * Method which is used to parse output from the node health monitor and
     * send to the report address.
     *
     * The timed out script or script which causes IOException output is
     * ignored.
     *
     * The node is marked unhealthy if
     * <ol>
     * <li>The node health script times out</li>
     * <li>The node health scripts output has a line which begins
     * with ERROR</li>
     * <li>An exception is thrown while executing the script</li>
     * </ol>
     * If the script throws {@link IOException} or {@link ExitCodeException} the
     * output is ignored and node is left remaining healthy, as script might
     * have syntax error.
     *
     * @param status
     */
    void reportHealthStatus(HealthCheckerExitStatus status) {
      switch (status) {
      case SUCCESS:
      case FAILED_WITH_EXIT_CODE:
        // see Javadoc above - we don't report bad health intentionally
        setHealthyWithoutReport();
        break;
      case TIMED_OUT:
        setUnhealthyWithReport(NODE_HEALTH_SCRIPT_TIMED_OUT_MSG);
        break;
      case FAILED_WITH_EXCEPTION:
        setUnhealthyWithReport(exceptionStackTrace);
        break;
      case FAILED:
        setUnhealthyWithReport(commandExecutor.getOutput());
        break;
      default:
        LOG.warn("Unknown HealthCheckerExitStatus - ignored.");
        break;
      }
    }

    /**
     * Method to check if the output string has line which begins with ERROR.
     *
     * @param output the output of the node health script to process
     * @return true if output string has error pattern in it.
     */
    private boolean hasErrors(String output) {
      String[] splits = output.split("\n");
      for (String split : splits) {
        if (split.startsWith(ERROR_PATTERN)) {
          return true;
        }
      }
      return false;
    }
  }

  @Override
  public void serviceStop() throws Exception {
    if (commandExecutor != null) {
      Process p = commandExecutor.getProcess();
      if (p != null) {
        p.destroy();
      }
    }
    super.serviceStop();
  }

  /**
   * Method used to determine whether the {@link NodeHealthScriptRunner}
   * should be started or not.<p>
   * Returns true if following conditions are met:
   *
   * <ol>
   * <li>Path to Node health check script is not empty</li>
   * <li>Node health check script file exists</li>
   * </ol>
   *
   * @return true if node health monitoring service can be started.
   */
  static boolean shouldRun(String script, String healthScript) {
    if (healthScript == null || healthScript.trim().isEmpty()) {
      LOG.info("Missing location for the node health check script \"{}\".",
          script);
      return false;
    }
    File f = new File(healthScript);
    if (!f.exists()) {
      LOG.warn("File {} for script \"{}\" does not exist.",
          healthScript, script);
      return false;
    }
    if (!FileUtil.canExecute(f)) {
      LOG.warn("File {} for script \"{}\" can not be executed.",
          healthScript, script);
      return false;
    }
    return true;
  }
}
