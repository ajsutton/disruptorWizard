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

import java.util.concurrent.CountDownLatch;

public class BatchHandlerStub implements BatchHandler<StubEntry>
{
    private final CountDownLatch countDownLatch;

    public BatchHandlerStub(final CountDownLatch countDownLatch)
    {
        this.countDownLatch = countDownLatch;
    }

    public void onAvailable(final StubEntry entry) throws Exception
    {
        countDownLatch.countDown();
    }

    public void onEndOfBatch() throws Exception
    {
    }
}
