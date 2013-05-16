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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.ConfigurationManager;
import org.apache.hadoop.conf.Configurator;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.FieldType;
import org.apache.hadoop.mapreduce.TaskType;

public class CompletedTasksTrainer extends Configured implements
    Trainer<JobDurationInfo> {

  private static final Log LOG = LogFactory.getLog(CompletedTasksTrainer.class);
  ConfigurationManager<CompletedTasksTrainer> cm;
  Set<JobInProgress> jips;
  int threshold;
  JobDurationInfoFactory factory;
  private int minTasks;
  private Map<JobID, JobDurationInfo> jipToMapDuration;
  private Map<JobID, JobDurationInfo> jipToReduceDuration;

  public CompletedTasksTrainer(JobDurationInfoFactory mapFactory
                             , TaskType type)
                                 throws IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {
    super();
    
    this.jips = new HashSet<JobInProgress>();
    this.jipToMapDuration = new HashMap<JobID, JobDurationInfo>();
    this.jipToReduceDuration = new HashMap<JobID, JobDurationInfo>();
    this.factory = mapFactory;
    
    this.cm = ConfigurationManager.createFor(this);
    this.cm.addConfiguratorFor(
        FieldType.Integer
      , type == TaskType.MAP ? CompletedTasksTrainer.NUM_MAP_COMPLETED_KEY
                             : CompletedTasksTrainer.NUM_REDUCE_COMPLETED_KEY
      , "Number of completed mappers needed to calculate the job size"
      , 2
      , new Configurator<Integer,CompletedTasksTrainer>() {
                 protected void set(CompletedTasksTrainer obj, Integer value) {
                   obj.threshold = value;
                 }
        });
    this.cm.addConfiguratorFor(
        FieldType.Integer
      , type == TaskType.MAP ? CompletedTasksTrainer.MIN_MAP_KEY
                             : CompletedTasksTrainer.MIN_REDUCE_KEY
      , "Under this number a job is considered small and the size is set to the minimum"
      , 1
      , new Configurator<Integer,CompletedTasksTrainer>() {
          protected void set(CompletedTasksTrainer obj, Integer value) {
            obj.minTasks = value;
          }
        });
  }
  
  public CompletedTasksTrainer(TaskType type,
                               Configuration conf,
                               JobDurationInfoFactory mapFactory) throws IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {
    this(mapFactory, type);
    
    LOG.debug("set threshold: " + threshold + " and minTasks: " + minTasks);
  }
  
  @Override
  public void followJob(JobInProgress jip, TaskType type) {
    this.jips.add(jip);
  }

  @Override
  public void unfollowJob(JobInProgress jip, TaskType type) {
    this.jips.remove(jip);
  }

  @Override
  public boolean isReady(JobInProgress jip, TaskType type) {

    int numTasks = type == TaskType.MAP ? jip.numMapTasks : jip.numReduceTasks;

    if (!jip.inited()) {
      return false;
    }

    if (numTasks < this.minTasks) {// Small jobs
      LOG.debug(jip.getJobID() + " is too small (" + numTasks + " < "
          + this.minTasks + ") => taking the shortcut");
      return true;
    }

    if (jip.getStatus().getRunState() != JobStatus.RUNNING) {
      LOG.debug(jip.getJobID() + " is not RUNNING");
      return false;
    }

    TaskInProgress[] tasks = jip.getTasks(type);

    if (tasks == null) {
      LOG.debug(jip.getJobID() + " tasks are null");
      return false;
    }

    float numCompleted = 0.0f;
    for (TaskInProgress task : tasks) {
      if (task != null && task.isComplete())
        numCompleted += 1.0;
    }

    float progress = numCompleted / tasks.length;
    LOG.debug("  isReady(" + jip.getJobID() + ":" + type + "):"
        + " num completed: " + numCompleted + " required: " + this.threshold
        + " progress: " + progress + " threshold: " + threshold);
    // return progress >= this.threshold;
    return numCompleted >= this.threshold;
  }

  @Override
  public JobDurationInfo getJobDurationInfo(JobInProgress jip, TaskType type) {
    int numTasks = type == TaskType.MAP ? jip.numMapTasks : jip.numReduceTasks;
    boolean isMap = type == TaskType.MAP;
    Map<JobID, JobDurationInfo> jipToDuration = isMap ? this.jipToMapDuration
        : this.jipToReduceDuration;
    if (jipToDuration.containsKey(jip.getJobID()))
      return jipToDuration.get(jip.getJobID());

    JobDurationInfo jobDurationInfo = null;
    if (numTasks < this.minTasks) // Small jobs
      jobDurationInfo = new UniformJobDurationInfo(jip, 0, type);
    else
      jobDurationInfo = this.factory.createJobDurationInfo(jip, type);
    jipToDuration.put(jip.getJobID(), jobDurationInfo);
    return jobDurationInfo;
  }

  @Override
  public void update(TaskTrackerManager taskTrackerManager) {
    // Nothing to do, in constant trainer we don't have temporary data
  }

  public final static String NUM_MAP_COMPLETED_KEY = getNumCompletedKeyFor(TaskType.MAP);
  public final static String NUM_REDUCE_COMPLETED_KEY = getNumCompletedKeyFor(TaskType.REDUCE);
  public final static String MIN_MAP_KEY = getMinKeyFor(TaskType.MAP);
  public final static String MIN_REDUCE_KEY = getMinKeyFor(TaskType.REDUCE);

  public final static String COMPLETED_TASKS_KEY = HFSPScheduler.TRAINER_KEYNAME
      + "completed-tasks";
  
  public static String getNumCompletedKeyFor(TaskType type) {
    return COMPLETED_TASKS_KEY + "." + type.toString().toLowerCase()
                               + ".num-completed";
  }

  public static String getMinKeyFor(TaskType type) {
    return type == TaskType.MAP ? HFSPScheduler.TRAINER_MIN_MAPS_KEYNAME
                                : HFSPScheduler.TRAINER_MIN_REDUCES_KEYNAME;
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    if (conf != null) {
      this.cm.configure(this.getConf());
    }
  }
  
  @Override
  public void printConfiguration() {
    System.out.println(this.cm.toString());
  }

  public static void main(String[] args) throws IllegalArgumentException, InstantiationException, IllegalAccessException, InvocationTargetException {
    CompletedTasksTrainer mapTrainer = new CompletedTasksTrainer(null, TaskType.MAP);
    CompletedTasksTrainer reduceTrainer = new CompletedTasksTrainer(null, TaskType.REDUCE);
    mapTrainer.printConfiguration();
    reduceTrainer.printConfiguration();
  }
}
