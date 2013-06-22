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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskType;

public class HFSPJIPListener extends JobInProgressListener {

  private final static Log LOG = LogFactory.getLog(HFSPJIPListener.class);

  private HFSPScheduler scheduler;

  public HFSPJIPListener(HFSPScheduler scheduler) {
    this.scheduler = scheduler;
  }

  /** Add jobs in training mode, then update its state */
  @Override
  public void jobAdded(JobInProgress jip) {
    if (jip.inited()) {
      LOG.debug(jip.getJobID() + " added");
      this.scheduler.jIDToJIP.put(jip.getJobID(), jip);
      for (TaskType type : HFSPScheduler.TASK_TYPES) {
        this.scheduler.addTrainJob(jip, type);
      }
    }
  }

  @Override
  public void jobRemoved(JobInProgress jip) {
    this.scheduler.removeJobIfCompleted(jip);
  }

  @Override
  public void jobUpdated(JobChangeEvent event) {
    JobInProgress jip = event.getJobInProgress();
    if (!this.scheduler.jIDToJIP.containsKey(jip.getJobID()) && jip.inited()) {
      this.jobAdded(jip);
    }
  }

}
