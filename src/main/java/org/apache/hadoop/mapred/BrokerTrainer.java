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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.TaskType;

public class BrokerTrainer extends Configured implements
    Trainer<JobDurationInfo> {

  private final static Log LOG = LogFactory.getLog(BrokerTrainer.class);

  Trainer<JobDurationInfo> mapTrainer;
  Trainer<JobDurationInfo> reduceTrainer;
  JobDurationInfoFactory mapFactory;
  //JobDurationInfoFactory reduceFactory;

  // TODO: virtual progress manager should not be used by brokerTrainer, fix
  // this
  public BrokerTrainer(HFSPScheduler scheduler, Configuration conf, Clock clock)
      throws IllegalArgumentException, InstantiationException,
      IllegalAccessException, InvocationTargetException {
    super(conf);

    // MAP conf
    this.mapFactory = new CompletedTasksDurationFactory(scheduler, 0, 0);
    this.mapTrainer = new CompletedTasksTrainer(TaskType.MAP, conf,
        this.mapFactory);

    // REDUCE
    SojournEstimator estimator = new SojournEstimator();
//    this.reduceFactory = new TasksProgressDurationFactory(TaskType.REDUCE,
//        conf, scheduler, estimator, clock);
    this.reduceTrainer = new SojournTrainer(TaskType.REDUCE, conf, estimator
                                          , clock);
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    
    if (this.mapFactory != null) {
      this.mapFactory.setConf(conf);
    }
    
    if (this.mapTrainer != null) {
      this.mapTrainer.setConf(conf);
    }
    
//    if (this.reduceFactory != null) {
//      this.reduceFactory.setConf(conf);
//    }
    
    if (this.reduceTrainer != null) {
      this.reduceTrainer.setConf(conf);
    }
  }
  
  private Trainer<JobDurationInfo> getTrainer(TaskType type) {
    if (type == TaskType.MAP)
      return this.mapTrainer;
    else
      return this.reduceTrainer;
  }

  @Override
  public void followJob(JobInProgress jip, TaskType type) {
    this.getTrainer(type).followJob(jip, type);
  }

  @Override
  public void unfollowJob(JobInProgress jip, TaskType type) {
    this.getTrainer(type).unfollowJob(jip, type);
  }

  @Override
  public boolean isReady(JobInProgress jip, TaskType type) {
    return this.getTrainer(type).isReady(jip, type);
  }

  @Override
  public JobDurationInfo getJobDurationInfo(JobInProgress jip, TaskType type) {
    Trainer<JobDurationInfo> trainer = this.getTrainer(type);
    LOG.debug("forwarding getJobDurationInfo to " + type + " trainer " + trainer.getClass());
    return trainer.getJobDurationInfo(jip, type);
  }

  @Override
  public void update(TaskTrackerManager taskTrackerManager) {
    this.mapTrainer.update(taskTrackerManager);
    this.reduceTrainer.update(taskTrackerManager);
  }

  @Override
  public void printConfiguration() {
    this.mapTrainer.printConfiguration();
    this.reduceTrainer.printConfiguration();
  }
}
