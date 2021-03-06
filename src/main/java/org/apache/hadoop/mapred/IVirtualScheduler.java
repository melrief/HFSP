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

import java.util.Collection;

import org.apache.hadoop.mapreduce.TaskType;

public interface IVirtualScheduler<JobDurationInfoClass extends JobDurationInfoBase<TaskDurationInfoClass>, TaskDurationInfoClass extends TaskDurationInfoBase> {

  VirtualCluster<TaskDurationInfoClass> schedule(final TaskType type,
      Collection<JobDurationInfoClass> jobs,
      VirtualCluster<TaskDurationInfoClass> cluster);
}
