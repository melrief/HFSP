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

public abstract class TaskDurationInfoBase {

  private boolean running;
  private TaskID taskID;

  public TaskDurationInfoBase(final TaskID taskID) {
    this.taskID = taskID;
    this.running = false;
  }

  public final void setRunning(final boolean running) {
    this.running = running;
  }

  public final boolean isRunning() {
    return running;
  }

  public abstract boolean isFinished();

  public final TaskID getTaskID() {
    return taskID;
  }
}
