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

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.TaskType;

public class JobDurationInfoFactory extends Configured implements
    JobDurationInfoBaseFactory<JobDurationInfo> {

  @Override
  public JobDurationInfo createJobDurationInfo(JobInProgress jip, TaskType type) {
    return createBenchJobDurationInfo(jip, type);
  }

  public static JobDurationInfo createBenchJobDurationInfo(JobInProgress jip,
      TaskType type) {
    JobConf jobConf = jip.getJobConf();
    TaskInProgress[] tips = jip.getTasks(type);
    Map<TaskID, TaskDurationInfo> tasks = new HashMap<TaskID, TaskDurationInfo>();

    for (TaskInProgress tip : tips) {
      tasks.put(tip.getTIPId(), new TaskDurationInfo(tip.getTIPId(), 0));
    }

    return new JobDurationInfo(jip.getJobID(), tasks, type);
  }

}
