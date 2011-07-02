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
