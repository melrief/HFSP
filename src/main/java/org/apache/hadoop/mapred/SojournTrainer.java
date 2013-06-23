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
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ConfigurationDescriptionToXMLConverter;
import org.apache.hadoop.conf.ConfigurationManager;
import org.apache.hadoop.conf.Configurator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.FieldType;
import org.apache.hadoop.mapreduce.TaskType;

public class SojournTrainer extends Configured implements
    Trainer<JobDurationInfo> {

  static final Log LOG = LogFactory.getLog(SojournTrainer.class);
  ConfigurationManager<SojournTrainer> configurationManager;
  private int minTasks;
  private long threshold;
  private Clock clock;
  private Map<JobID, Long> jipToMapEstimatedTime;
  private Map<JobID, Long> jipToReduceEstimatedTime;
  private Map<JobID, JobDurationInfo> jipToMapDuration;
  private Map<JobID, JobDurationInfo> jipToReduceDuration;

  public SojournTrainer(TaskType type, Configuration conf, Clock clock)
      throws IllegalArgumentException, InstantiationException,
      IllegalAccessException, InvocationTargetException {

    this.configurationManager = ConfigurationManager.createFor(this);
    this.configurationManager
        .addConfiguratorFor(
            FieldType.Long,
            SojournTrainer.getSojournConfKeyname(HFSPScheduler.PREFIX_KEYNAME,
                type),
            "a task with at least this sojourn time can be used for the job size estimation",
            Long.MAX_VALUE, new Configurator<Long, SojournTrainer>() {
              protected void set(SojournTrainer obj, Long value) {
                obj.threshold = value;
              }
            });

    // this.minTasks = conf.getInt("mapred.hfsp-scheduler.trainer.min-reduces",
    // 0);
    this.configurationManager.addConfiguratorFor(FieldType.Integer,
        type == TaskType.MAP ? HFSPScheduler.TRAINER_MIN_MAPS_KEYNAME
            : HFSPScheduler.TRAINER_MIN_REDUCES_KEYNAME,
        "jobs with a number of tasks smaller than this avoid the train "
            + "and their size  is set to the smallest possible", 0,
        new Configurator<Integer, SojournTrainer>() {
          protected void set(SojournTrainer obj, Integer value) {
            obj.minTasks = value;
          }
        });

    this.setConf(conf);

    this.clock = clock;
    this.jipToMapEstimatedTime = new HashMap<JobID, Long>();
    this.jipToReduceEstimatedTime = new HashMap<JobID, Long>();
    this.jipToMapDuration = new HashMap<JobID, JobDurationInfo>();
    this.jipToReduceDuration = new HashMap<JobID, JobDurationInfo>();
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null) {
      this.configurationManager.configure(conf);
    }
  }

  @Override
  public boolean isReady(JobInProgress jip, TaskType type) {

    Map<JobID, Long> jipToEstimatedTime = this.getJipToEstimatedTime(type);

    if (!jip.inited()) {
      return false;
    }

    // already calculated
    if (jipToEstimatedTime.containsKey(jip.getJobID()))
      return true;

    if (jip.getStatus().getRunState() != JobStatus.RUNNING)
      return false;

    final long currentTime = this.clock.getTime();
    long estimatedTime = this.getEstimatedTime(jip, type, currentTime);
    if (estimatedTime > -1)
      jipToEstimatedTime.put(jip.getJobID(), estimatedTime);

    return estimatedTime > -1;
  }

  public static long getSojournTime(TaskInProgress task, long time) {
    long finishTime = 0;
    if (task.isMapTask()) {
      finishTime = task.getExecFinishTime();
      return (finishTime == 0) ? time - task.getExecStartTime() : task
          .getExecFinishTime() - task.getExecStartTime();
    } else {
      long sortFinishTime = 0;
      for (TaskStatus status : task.getTaskStatuses()) {
        sortFinishTime = status.getSortFinishTime();
        if (sortFinishTime == 0)
          continue;
        finishTime = status.getFinishTime();
        // if (finishTime == 0 || finishTime == sortFinishTime)
        if (finishTime == 0) {
          SojournTrainer.LOG.debug(task.getTIPId() + " time: " + time
              + " sortFinishTime: " + sortFinishTime + " finishTime: 0");
          return time - sortFinishTime;
        }
        SojournTrainer.LOG.debug(task.getTIPId() + " time: " + time
            + " sortFinishTime: " + sortFinishTime + " finishTime: "
            + finishTime);
        return finishTime - sortFinishTime;
      }
    }
    return 0;
  }
  
  /**
   * @return the estimated finish time for the JobInProgress jip
   */
  private long getEstimatedTime(JobInProgress jip, TaskType type,
      long currentTime) {
    boolean isMap = type == TaskType.MAP;

    if (!isMap && !jip.scheduleReduces()) {
      LOG.debug(jip.getJobID() + ".scheduleReduces() = false => no sojourn"
          + " estimation can be done");
      return -1;
    }

    // int numTasks = isMap ? jip.numMapTasks : jip.numReduceTasks;

    // Small jobs, no need of training
    // FIXME: fix the shortcut with a valid value
    // if (numTasks < this.minTasks) {
    // LOG.debug(jip.getJobID() + " is too small (" + numTasks + " < "
    // + this.minTasks + ") => taking the shortcut");
    // return 0l;
    // }

    TaskInProgress[] tasks = jip.getTasks(type);
    if (tasks == null) {
      LOG.debug(jip.getJobID() + " task lists is null, no sojourn estimation"
          + "	can be done");
      return -1;
    }

    Map<TaskInProgress, Long> tipToSojournTime = new HashMap<TaskInProgress, Long>();

    for (TaskInProgress task : tasks) {
      if (task == null)
        continue;

      if (task.isComplete()) {
        // clean this
        long currentExecTime = SojournTrainer.getSojournTime(task, currentTime);
        tipToSojournTime.put(task, currentExecTime);
      }

      if (!task.isRunning())
        continue;

      long currentExecTime = SojournTrainer.getSojournTime(task, currentTime);

      if (currentExecTime >= this.threshold) {
        // task.getProgress() for reduces returns the total reduce phase
        // progress. Each reduce phase (shuffle, sort, reduce) has 33%
        // of the total progress, so the reduce phase of the reduce is
        // task.getProgress() - (2 / 3) that is a value between 0 and 33.
        // Then * 3 we have a value from 0 to 1
        double currentProgress = task.getProgress() - (2.0 / 3.0);
        if (currentProgress <= 0.0)
          continue;

        double reduceExecProgress = currentProgress * 3.0;
        long taskSojournTime = (long) Math.ceil(currentExecTime
            / reduceExecProgress);
        LOG.debug(task.getTIPId() + ": task.getProgress(): "
            + task.getProgress() + " - (2.0 / 3.0): " + currentProgress
            + " *3: " + reduceExecProgress + " currentExecTime: "
            + currentExecTime + " => remainingSojournTime: " + taskSojournTime);
        tipToSojournTime.put(task, taskSojournTime);
      }
    }

    if (tipToSojournTime.size() >= this.minTasks) {
      long totalSojournTime = 0;
      for (long sojournTime : tipToSojournTime.values()) {
        totalSojournTime += sojournTime;
      }
      if (tipToSojournTime.isEmpty())
        return -1;
      else {
        LOG.debug(jip.getJobID() + " totalSojournTime: " + totalSojournTime
            + " tipToSojournTime.size(): " + tipToSojournTime.size() + " => "
            + (totalSojournTime / tipToSojournTime.size()));
        return totalSojournTime / tipToSojournTime.size();
      }
    } else {
      LOG.debug(jip.getJobID() + " has not enough tasks to estimate its"
          + " size (" + tipToSojournTime.size() + " < " + this.minTasks + ")");
      return -1;
    }
  }

  @Override
  public void followJob(JobInProgress jip, TaskType type) {
    // TODO Auto-generated method stub

  }

  @Override
  public void unfollowJob(JobInProgress jip, TaskType type) {
    // TODO Auto-generated method stub

  }

  @Override
  public JobDurationInfo getJobDurationInfo(JobInProgress jip, TaskType type) {
    boolean isMap = type == TaskType.MAP;
    Map<JobID, JobDurationInfo> jipToDuration = isMap ? this.jipToMapDuration
        : this.jipToReduceDuration;

    if (jipToDuration.containsKey(jip.getJobID()))
      return jipToDuration.get(jip.getJobID());

    Map<JobID, Long> jipToEstimatedTime = this.getJipToEstimatedTime(type);

    // not ready job or not yet updated value
    if (jipToEstimatedTime.get(jip.getJobID()) == null) {
      // FIXME separate isReady from update (this isReady call serves to update
      // trainer internal status
      if (!this.isReady(jip, type)) {
        return null;
      }
    }

    if (jipToEstimatedTime.get(jip.getJobID()) == -1)
      return null;

    // ready job
    JobDurationInfo jobDurationInfo = new UniformJobDurationInfo(jip,
        jipToEstimatedTime.get(jip.getJobID()), type);
    jipToDuration.put(jip.getJobID(), jobDurationInfo);
    return jobDurationInfo;
  }

  public Map<JobID, Long> getJipToEstimatedTime(TaskType type) {
    return type == TaskType.MAP ? this.jipToMapEstimatedTime
        : this.jipToReduceEstimatedTime;
  }

  @Override
  public void update(TaskTrackerManager taskTrackerManager) {
    // TODO Auto-generated method stub

  }

  public static String getSojournConfKeyname(String prefixKeyname, TaskType type) {
    return prefixKeyname + "sojourn-trainer." + type.toString().toLowerCase()
        + ".time";
  }

  @Override
  public void accept(ConfigurationDescriptionToXMLConverter converter) {
    this.configurationManager.accept(converter);
  }
}
