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

import java.io.IOException;

public class UpdateThread extends Thread {

  HFSPScheduler scheduler;
  private Boolean shouldStop;

  public UpdateThread(HFSPScheduler scheduler) {
    if (scheduler == null) {
      throw new NullPointerException("scheduler cannot be null");
    }
    this.scheduler = scheduler;
    this.shouldStop = Boolean.FALSE;
  }

  @Override
  public void run() {
    while (true) {
      try {
        // check should stop
        synchronized (this.shouldStop) {
          if (this.shouldStop)
            break;
        }

        long updateInterval;

        // Let the user change the update interval
        synchronized (this.scheduler.updateInterval) {
          updateInterval = this.scheduler.updateInterval;
        }
        Thread.sleep(updateInterval);

        // update the scheduler status
        this.scheduler.update();

      } catch (InterruptedException e) {
        e.printStackTrace();
        break;
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  public void terminate() {
    synchronized (this.shouldStop) {
      this.shouldStop = true;
    }
  }

}
