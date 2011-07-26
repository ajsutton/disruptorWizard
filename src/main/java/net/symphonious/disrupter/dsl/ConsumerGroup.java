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
package net.symphonious.disrupter.dsl;

import com.lmax.disruptor.AbstractEntry;
import com.lmax.disruptor.BatchHandler;
import com.lmax.disruptor.Consumer;

/** A group of consumers set up via the {@link DisruptorWizard}.
 *
 * @param <T> the type of entry used by the consumers.
 */
public class ConsumerGroup<T extends AbstractEntry>
{
    private final DisruptorWizard<T> disruptorWizard;
    private final Consumer[] consumers;

    ConsumerGroup(final DisruptorWizard<T> disruptorWizard, final Consumer[] consumers)
    {
        this.disruptorWizard = disruptorWizard;
        this.consumers = consumers;
    }

    /** Set up batch handlers to consume events from the ring buffer. These handlers will only process events
     *  after every consumer in this group has processed the event.
     *
     *  <p>This method is generally used as part of a chain. For example if the handler <code>A</code> must
     *  process events before handler <code>B</code>:</p>
     *
     *  <pre><code>dw.consumeWith(A).then(B);</code></pre>
     *
     * @param handlers the batch handlers that will consume events.
     * @return a {@link ConsumerGroup} that can be used to set up a consumer barrier over the created consumers.
     */
    public ConsumerGroup<T> then(final BatchHandler<T>... handlers)
    {
        return consumeWith(handlers);
    }

    /** Set up batch handlers to consume events from the ring buffer. These handlers will only process events
     *  after every consumer in this group has processed the event.
     *
     *  <p>This method is generally used as part of a chain. For example if the handler <code>A</code> must
     *  process events before handler <code>B</code>:</p>
     *
     *  <pre><code>dw.after(A).consumeWith(B);</code></pre>
     *
     * @param handlers the batch handlers that will consume events.
     * @return a {@link ConsumerGroup} that can be used to set up a consumer barrier over the created consumers.
     */
    public ConsumerGroup<T> consumeWith(final BatchHandler<T>... handlers)
    {
        return disruptorWizard.createConsumers(consumers, handlers);
    }
}
