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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapreduce.TaskType;

public class CompletedTasksDurationFactory extends JobDurationInfoFactory {

  private final static Log LOG = LogFactory
      .getLog(CompletedTasksDurationFactory.class);

  private HFSPScheduler scheduler;

  private float errorOnMapEstimation;

  private long seed;

  private Random random;

  public CompletedTasksDurationFactory(HFSPScheduler scheduler,
      float errorOnMapEstimation, long seed) {
    super();
    this.scheduler = scheduler;
    this.errorOnMapEstimation = errorOnMapEstimation;
    this.seed = seed;
    this.random = new Random(this.seed);
  }

  public static double sum(Iterable<Double> iter) {
    double res = 0;
    for (Double d : iter) {
      res += d;
    }
    return res;
  }

  /**
   * Return the job duration info if it is possible to calculate it based on the
   * completion time of each task or null if it is not possible
   */
  @Override
  public JobDurationInfo createJobDurationInfo(JobInProgress jip, TaskType type) {

    JobDurationInfo currJobDuration = this.scheduler.getDuration(
        jip.getJobID(), type);
    List<Double> completionTimes = new LinkedList<Double>();
    Map<TaskID, Double> tIDToVirtualProgress = new HashMap<TaskID, Double>();

    for (TaskInProgress task : jip.getTasks(type)) {

      if (task.isComplete()) {

        TaskAttemptID attemptID = task.getSuccessfulTaskid();

        if (attemptID == null) {
          LOG.error("No attempt found for " + task.getTIPId() + ", skipping");
          continue;
        }

        TaskStatus completedAttemptStatus = task.getTaskStatus(attemptID);

        LOG.debug(task.getTIPId() + " completed using "
            + completedAttemptStatus.getTaskID()
            + ", calculating completion time");

        double taskCompletionTime = (type == TaskType.MAP ? this
            .getMapCompletionTime(completedAttemptStatus) : this
            .getReduceCompletionTime(completedAttemptStatus));

        completionTimes.add(taskCompletionTime);
      }
      TaskDurationInfo tdi = currJobDuration.getTasks().get(task.getTIPId());
      tIDToVirtualProgress.put(task.getTIPId(), (double) tdi.getDuration()
          / (double) tdi.getTotalDuration());
    }

    if (completionTimes.size() == 0) {
      LOG.error("cannot estimate duration of " + jip.getJobID() + ":" + type
          + " because there are no finished tasks");
      return null;
    }

    /* duration that only consider completed tasks */
    long totalDuration = (long) (Math.ceil(sum(completionTimes)
        / completionTimes.size()));

    /* add error to the final estimation */
    boolean isPos = this.random.nextBoolean();
    float absError = this.random.nextFloat() * this.errorOnMapEstimation;
    float error = totalDuration * absError;
    long totalDurationWithError = 0;
    if (isPos) {
      totalDurationWithError = (long) Math.ceil((double) totalDuration + error);
    } else if (error <= totalDuration) {
      totalDurationWithError = (long) Math
          .floor((double) totalDuration - error);
    }

    /* Add virtual progress to each task */
    Map<TaskID, TaskDurationInfo> IDToDuration = new HashMap<TaskID, TaskDurationInfo>();
    for (Entry<TaskID, Double> entry : tIDToVirtualProgress.entrySet()) {
      TaskID taskID = entry.getKey();
      double prog = entry.getValue();
      TaskDurationInfo tdi = new TaskDurationInfo(taskID,
          totalDurationWithError);
      long workDone = (long) Math.ceil(tdi.getTotalDuration() * prog);
      tdi.decrease(workDone);
      IDToDuration.put(taskID, tdi);
    }

    JobDurationInfo jobDurationInfo = new JobDurationInfo(jip.getJobID(),
        IDToDuration, type);

    if (LOG.isDebugEnabled()) {
      LOG.debug(jip.getJobID() + " has " + completionTimes.size() + "/"
          + (completionTimes.size() + tIDToVirtualProgress.size())
          + " completed " + type + " tasks with completion times: "
          + completionTimes + " and virtual progress for each task: "
          + tIDToVirtualProgress + " => remaining duration for each task: "
          + totalDuration + " , absError: " + absError + ", error: " + error
          + " => final estimation: " + totalDurationWithError
          + " curr virtual progress:" + jobDurationInfo);
    }

    TaskID[] taskIDs = new TaskID[tIDToVirtualProgress.size()];
    tIDToVirtualProgress.keySet().toArray(taskIDs);

    return new UniformJobDurationInfo(jip, totalDurationWithError, type,
        taskIDs);

  }

  private double getReduceCompletionTime(TaskStatus status) {
    long startTime = status.getStartTime();
    long finishTime = status.getFinishTime();
    long shuffleTime = status.getShuffleFinishTime();
    long sortTime = status.getSortFinishTime();

    LOG.debug(status.getTaskID() + " startTime: " + startTime
        + " shuffleTime: " + shuffleTime + " sortTime: " + sortTime
        + " finishTime: " + finishTime + " => completionTime: "
        + (finishTime - sortTime));

    assert finishTime > 0 && shuffleTime > 0 && sortTime > 0;

    return finishTime - sortTime;
  }

  private double getMapCompletionTime(TaskStatus status) {
    LOG.debug(status.getTaskID() + " startTime: " + status.getStartTime()
        + " finishTime: " + status.getFinishTime() + " => completionTime: "
        + (status.getFinishTime() - status.getStartTime()));
    return status.getFinishTime() - status.getStartTime();
  }

}
