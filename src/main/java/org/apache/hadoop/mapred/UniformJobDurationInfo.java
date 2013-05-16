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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskType;

public class UniformJobDurationInfo extends JobDurationInfo {

  private static final Log LOG = LogFactory
      .getLog(UniformJobDurationInfo.class);

  public final static TaskID[] getTaskIDS(TaskInProgress[] tips) {
    TaskID[] taskIDS = new TaskID[tips.length];
    for (int i = 0; i < tips.length; i++)
      taskIDS[i] = tips[i].getTIPId();
    return taskIDS;
  }

  public final static TaskInProgress[] getNotFinishedTasks(JobInProgress jip,
      TaskType type) {
    List<TaskInProgress> notFinishedTasks = new LinkedList<TaskInProgress>();
    for (TaskInProgress tip : jip.getTasks(type)) {
      if (!tip.isComplete())
        notFinishedTasks.add(tip);
    }
    TaskInProgress[] res = new TaskInProgress[notFinishedTasks.size()];
    return notFinishedTasks.toArray(res);
  }

  /** Create a duration containing not completed tasks with taskDuration */
  public UniformJobDurationInfo(JobInProgress jip, long taskDuration,
      TaskType type) {
    this(jip, taskDuration, type, getTaskIDS(getNotFinishedTasks(jip, type)));
  }

  public UniformJobDurationInfo(JobInProgress jip, long taskDuration,
      TaskType type, TaskID[] tips) {
    super(jip.getJobID(), createUniformTaskDurationInfos(tips,
        (long) Math.ceil(taskDuration)), type);
    LOG.debug("created uniform job duration of " + this.getPhaseTotalDuration()
        + " for " + this.getJobID() + ":" + type + " with "
        + jip.getTasks(type).length + " tasks (taskDuration: " + taskDuration
        + ", inited: " + jip.inited() + ")");
  }

  private static Map<TaskID, TaskDurationInfo> createUniformTaskDurationInfos(
      TaskID[] tasks, final long singleTaskDuration) {
    Map<TaskID, TaskDurationInfo> result = new HashMap<TaskID, TaskDurationInfo>();

    for (TaskID task : tasks) {
      result.put(task, new TaskDurationInfo(task, singleTaskDuration));
    }

    return result;
  }
}
