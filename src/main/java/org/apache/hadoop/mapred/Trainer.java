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

import org.apache.hadoop.conf.PrintConfiguration;
import org.apache.hadoop.mapreduce.TaskType;

public interface Trainer<JobDurationInfoClass extends JobDurationInfoBase<? extends TaskDurationInfoBase>>
  extends PrintConfiguration {

  /**
   * register the phase (MAP, REDUCE) of a job to the trainer
   */
  void followJob(JobInProgress jip, TaskType type);

  /**
   * unregister the phase (MAP, REDUCE) of a job to the trainer
   */
  void unfollowJob(JobInProgress jip, TaskType type);

  /**
   * @return if the informations about the job are enough for size based
   *         scheduling
   */
  boolean isReady(JobInProgress jip, TaskType type);

  /**
   * @return the duration info about the phase of a job
   */
  JobDurationInfoClass getJobDurationInfo(JobInProgress jip, TaskType type);

  /**
   * Update followed job statuses based on the current state of the jobTracker
   */
  void update(TaskTrackerManager taskTrackerManager);
}
