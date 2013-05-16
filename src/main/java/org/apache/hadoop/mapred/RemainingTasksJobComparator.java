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

public class RemainingTasksJobComparator implements Comparator<JobInProgress> {

  private TaskType type;

  public RemainingTasksJobComparator(TaskType type) {
    this.type = type;
  }

  @Override
  public int compare(JobInProgress j1, JobInProgress j2) {
    boolean isMap = type == TaskType.MAP;
    int desired1 = isMap ? j1.desiredMaps() : j1.desiredReduces();
    int running1 = isMap ? j1.runningMaps() : j1.runningReduces();
    int finished1 = isMap ? j1.finishedMaps() : j1.finishedReduces();
    final Integer remaining1 = desired1 - (running1 + finished1);

    int desired2 = isMap ? j2.desiredMaps() : j2.desiredReduces();
    int running2 = isMap ? j2.runningMaps() : j2.runningReduces();
    int finished2 = isMap ? j2.finishedMaps() : j2.finishedReduces();
    final Integer remaining2 = desired2 - (running2 + finished2);

    return remaining1.compareTo(remaining2);
  }

}
