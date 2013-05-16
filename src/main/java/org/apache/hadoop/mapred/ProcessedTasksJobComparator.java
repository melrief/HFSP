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

import java.util.Comparator;

import org.apache.hadoop.mapreduce.TaskType;

/**
 * Compare JobInProgress based on the sum of running and finished tasks in
 * decreasing order
 */
public class ProcessedTasksJobComparator implements Comparator<JobInProgress> {

  final TaskType type;

  public ProcessedTasksJobComparator(TaskType type) {
    this.type = type;
  }

  @Override
  public int compare(JobInProgress j1, JobInProgress j2) {
    int j1IsNew = 0;
    int j2IsNew = 0;
    if (type == TaskType.MAP) {
      j1IsNew = j1.runningMaps() + j1.finishedMaps();
      j2IsNew = j2.runningMaps() + j2.finishedMaps();
    } else {
      j1IsNew = j1.runningReduces() + j1.finishedReduces();
      j2IsNew = j2.runningReduces() + j2.finishedReduces();
    }

    if (j1IsNew < j2IsNew) {
      return 1;
    }
    if (j1IsNew > j2IsNew) {
      return -1;
    }
    return j1.getJobID().compareTo(j2.getJobID());
  }

}