/* 
 * Copyright 2012 Eurecom (http://www.eurecom.fr)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.mapred;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ConfigurationManager;
import org.apache.hadoop.conf.Configurator;
import org.apache.hadoop.conf.FieldType;
import org.apache.hadoop.mapreduce.TaskType;

public class TasksProgressDurationFactory extends JobDurationInfoFactory {

  ConfigurationManager<TasksProgressDurationFactory> configurationManager;
  private long minSojourn;
  private SojournEstimator sojournEstimator;
  private Clock clock;
  private HFSPScheduler scheduler;
  private static Log LOG = LogFactory
      .getLog(TasksProgressDurationFactory.class);

  public TasksProgressDurationFactory(TaskType type,
      Configuration conf,
      HFSPScheduler scheduler,
      SojournEstimator sojournEstimator, Clock clock) throws IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {
    this.configurationManager = ConfigurationManager.createFor(this);
    
    // this.minSojourn = conf.getLong(SojournTrainer.getConfKeyname(
    //         HFSPScheduler.PREFIX_KEYNAME, TaskType.REDUCE), Long.MAX_VALUE);
    this.configurationManager.addConfiguratorFor(
        FieldType.Long
      , SojournTrainer.getSojournConfKeyname(HFSPScheduler.PREFIX_KEYNAME, type)
      , "a task with at least this sojourn time can be used for the job size estimation"
      , Long.MAX_VALUE
      , new Configurator<Long, TasksProgressDurationFactory>() {
          protected void set(TasksProgressDurationFactory obj, Long value) {
            obj.minSojourn = value;
          }
        });
    
    this.scheduler = scheduler;
    this.sojournEstimator = sojournEstimator;
    this.clock = clock;
    this.setConf(conf);
  }
  
  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null) {
      this.configurationManager.configure(conf);
    }
  }

  @Override
  public JobDurationInfo createJobDurationInfo(JobInProgress jip, TaskType type) {

    TaskInProgress[] tasks = jip.getTasks(type);

    if (tasks == null) {
      LOG.error("cannot estimate duration of " + jip.getJobID() + ":" + type
          + " because there are no finished tasks");
      return null;
    }

    long currentTime = this.clock.getTime();

    long biggestDuration = 0;
    TaskInProgress selectedTask = null;

    for (TaskInProgress task : tasks) {
      long estimatedTime = this.sojournEstimator.getSojournTime(task,
          currentTime);
      double progress = task.getProgress();

      if (progress == 0.0f)
        continue;

      if (estimatedTime > this.minSojourn) {
        biggestDuration = (long) Math.ceil(estimatedTime / progress);
        selectedTask = task;
        LOG.debug(jip.getJobID() + " estimatedTime: " + estimatedTime
            + " task.getProgress(): " + task.getProgress() + " total: "
            + biggestDuration + " (selected: " + task.getTIPId() + ")");
        break;
      } else if (task.isComplete()) {
        biggestDuration = 0;
        selectedTask = task;
        LOG.debug(jip.getJobID() + " is complete with time <= to minSojourn "
            + " total: 0 (selected: " + task.getTIPId() + ")");
        break;
      }
    }
    if (selectedTask == null) {
      LOG.error("cannot calculate duration of " + jip.getJobID()
          + ", returning duration == 0");
      return null;
    } else {

      JobDurationInfo jdi = this.scheduler.getDuration(jip.getJobID(), type);

      long workDone = 0;
      ArrayList<TaskID> tmpTaskIDs = new ArrayList<TaskID>();
      if (jdi != null) {
        for (Entry<TaskID, TaskDurationInfo> entry : jdi.getTasks().entrySet()) {
          TaskID taskID = entry.getKey();
          TaskDurationInfo tdi = entry.getValue();
          if (!tdi.isFinished()) {
            workDone = tdi.getTotalDuration() - tdi.getDuration();
            tmpTaskIDs.add(taskID);
          }
        }
      }

      TaskID[] taskIDs = new TaskID[tmpTaskIDs.size()];
      tmpTaskIDs.toArray(taskIDs);

      long totalDuration = (long) (biggestDuration - Math.ceil(workDone
          / taskIDs.length));

      LOG.debug(jip.getJobID() + " has biggest duration: " + biggestDuration
          + " for " + type + " tasks (virtual completed: " + taskIDs.length
          + ")" + " workDone from unfinished tasks: " + workDone
          + " => remaining duration for each task: " + totalDuration
          + " + span => final estimation: " + totalDuration);

      return new UniformJobDurationInfo(jip, biggestDuration, type, taskIDs);
    }
  }

}
