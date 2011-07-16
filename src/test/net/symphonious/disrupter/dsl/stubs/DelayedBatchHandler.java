/* Copyright 2011 Adrian Sutton
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
package net.symphonious.disrupter.dsl.stubs;

import com.lmax.disruptor.BatchHandler;

import java.util.concurrent.atomic.AtomicBoolean;

public class DelayedBatchHandler implements BatchHandler<TestEntry>
{
    private AtomicBoolean readyToProcessEvent = new AtomicBoolean(false);
    private volatile boolean stopped = false;

    public void onAvailable(final TestEntry entry) throws Exception
    {
        waitForAndSetFlag(false);
        System.out.println("Finished entry: " + entry.getSequence());
    }

    public void onEndOfBatch() throws Exception
    {
    }

    public void processEvent()
    {
        waitForAndSetFlag(true);
    }

    public void stopWaiting()
    {
        stopped = true;
    }

    private void waitForAndSetFlag(final boolean newValue)
    {
        while (!stopped && !Thread.interrupted() && !readyToProcessEvent.compareAndSet(!newValue, newValue))
        {
            Thread.yield();
        }
    }
}
