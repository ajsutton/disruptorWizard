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

import com.lmax.disruptor.*;

import java.util.concurrent.Executor;

/**
 * A DSL-style wizard for setting up the disruptor pattern around a ring buffer.
 * <p/>
 * <p>A simple example of setting up the disruptor with two consumers that must process
 * events in order:</p>
 * <p/>
 * <pre><code> DisruptorWizard<MyEntry> dw = new DisruptorWizard<MyEntry>(MyEntry.FACTORY, 32, Executors.newCachedThreadPool());
 * BatchHandler<MyEntry> handler1 = new BatchHandler<MyEntry>() { ... };
 * BatchHandler<MyEntry> handler2 = new BatchHandler<MyEntry>() { ... };
 * dw.consumeWith(handler1);
 * dw.after(handler1).consumeWith(handler2);
 * <p/>
 * ProducerBarrier producerBarrier = dw.getProducerBarrier();</code></pre>
 *
 * @param <T> the type of {@link Entry} used.
 */
public class DisruptorWizard<T extends AbstractEntry>
{
    private final RingBuffer<T> ringBuffer;
    private final Executor executor;
    private ExceptionHandler exceptionHandler;
    private ConsumerRepository<T> consumerRepository = new ConsumerRepository<T>();
    private ProducerBarrier<T> producerBarrier;

    /**
     * Create a new DisruptorWizard.
     *
     * @param entryFactory   the factory to create entries in the ring buffer.
     * @param ringBufferSize the size of the ring buffer.
     * @param executor       an {@link Executor} to execute consumers.
     */
    public DisruptorWizard(final EntryFactory<T> entryFactory, final int ringBufferSize, final Executor executor)
    {
        this(new RingBuffer<T>(entryFactory, ringBufferSize), executor);
    }

    /**
     * Create a new DisruptorWizard.
     *
     * @param entryFactory   the factory to create entries in the ring buffer.
     * @param ringBufferSize the size of the ring buffer.
     * @param executor       an {@link Executor} to execute consumers.
     * @param claimStrategy  the claim strategy to use for the ring buffer.
     * @param waitStrategy   the wait strategy to use for the ring buffer.
     */
    public DisruptorWizard(final EntryFactory<T> entryFactory, final int ringBufferSize, final Executor executor,
                           final ClaimStrategy.Option claimStrategy,
                           final WaitStrategy.Option waitStrategy)
    {
        this(new RingBuffer<T>(entryFactory, ringBufferSize, claimStrategy, waitStrategy), executor);
    }

    private DisruptorWizard(final RingBuffer<T> ringBuffer, final Executor executor)
    {
        this.ringBuffer = ringBuffer;
        this.executor = executor;
    }

    /**
     * Set up batch handlers to consume events from the ring buffer. These handlers will process events
     * as soon as they become available, in parallel.
     * <p/>
     * <p>This method can be used as the start of a chain. For example if the handler <code>A</code> must
     * process events before handler <code>B</code>:</p>
     * <p/>
     * <pre><code>dw.consumeWith(A).then(B);</code></pre>
     *
     * @param handlers the batch handlers that will consume events.
     * @return a {@link ConsumerGroup} that can be used to set up a consumer barrier over the created consumers.
     */
    public ConsumerGroup<T> consumeWith(final BatchHandler<T>... handlers)
    {
        return createConsumers(new Consumer[0], handlers);
    }

    /** Specify an exception handler to be used for any future consumers.
     * Note that only consumers set up after calling this method will use the exception handler.
     *
     * @param exceptionHandler the exception handler to use for any future consumers.
     */
    public void handleExceptionsWith(final ExceptionHandler exceptionHandler)
    {
        this.exceptionHandler = exceptionHandler;
    }

    /** Override the default exception handler for a specific batch handler.
     * <pre>disruptorWizard.handleExceptionsIn(batchHandler).with(exceptionHandler);</pre>
     *
     * @param batchHandler the batch handler to set a different exception handler for.
     * @return an ExceptionHandlerSetting dsl object - intended to be used by chaining the with method call.
     */
    public ExceptionHandlerSetting handleExceptionsFor(final BatchHandler<T> batchHandler)
    {
        return new ExceptionHandlerSetting<T>(batchHandler, consumerRepository);
    }

    /**
     * Specifies a group of consumers that can then be used to build a barrier for dependent consumers.
     * For example if the handler <code>A</code> must process events before handler <code>B</code>:
     * <p/>
     * <pre><code>dw.after(A).consumeWith(B);</code></pre>
     *
     * @param handlers the batch handlers, previously set up with {@link #consumeWith(com.lmax.disruptor.BatchHandler[])},
     *                 that will form the barrier for subsequent handlers.
     * @return a {@link ConsumerGroup} that can be used to setup a consumer barrier over the specified consumers.
     */
    public ConsumerGroup<T> after(final BatchHandler<T>... handlers)
    {
        Consumer[] selectedConsumers = new Consumer[handlers.length];
        for (int i = 0, handlersLength = handlers.length; i < handlersLength; i++)
        {
            final BatchHandler<T> handler = handlers[i];
            selectedConsumers[i] = consumerRepository.getConsumerFor(handler);
            if (selectedConsumers[i] == null)
            {
                throw new IllegalArgumentException("Batch handlers must be consuming from the ring buffer before they can be used in a barrier condition.");
            }
        }
        return new ConsumerGroup<T>(this, selectedConsumers);
    }

    /**
     * Create a producer barrier.  The barrier is set up to prevent overwriting any entry that is yet to
     * be processed by a consumer that has already been set up.  As such, producer barriers should be
     * created as the last step, after all handlers have been set up.
     *
     * @return the producer barrier.
     */
    public ProducerBarrier<T> getProducerBarrier()
    {
        if (producerBarrier == null)
        {
            startConsumers();
            producerBarrier = ringBuffer.createProducerBarrier(consumerRepository.getLastConsumersInChain());
        }
        return producerBarrier;
    }

    /** Get the consumer barrier used by a specific handler. Note that the consumer barrier
     * may be shared by multiple consumers.
     *
     * @param handler the handler to get the barrier for.
     * @return the ConsumerBarrier used by <i>handler</i> to retrieve entries.
     */
    public ConsumerBarrier<T> getBarrierFor(final BatchHandler<T> handler)
    {
        return consumerRepository.getBarrierFor(handler);
    }

    /**
     * Calls {@link com.lmax.disruptor.Consumer#halt()} on all of the consumers created via this wizard.
     */
    public void halt()
    {
        for (ConsumerInfo consumerInfo : consumerRepository)
        {
            consumerInfo.getConsumer().halt();
        }
    }

    ConsumerGroup<T> createConsumers(final Consumer[] barrierConsumers, final BatchHandler<T>[] batchHandlers)
    {
        if (producerBarrier != null)
        {
            throw new IllegalStateException("All consumers must be created before creating the producer barrier.");
        }
        final Consumer[] createdConsumers = new Consumer[batchHandlers.length];
        final ConsumerBarrier<T> barrier = ringBuffer.createConsumerBarrier(barrierConsumers);
        for (int i = 0, batchHandlersLength = batchHandlers.length; i < batchHandlersLength; i++)
        {
            final BatchHandler<T> batchHandler = batchHandlers[i];
            final BatchConsumer<T> batchConsumer = new BatchConsumer<T>(barrier, batchHandler);
            if (exceptionHandler != null)
            {
                batchConsumer.setExceptionHandler(exceptionHandler);
            }

            consumerRepository.add(batchConsumer, batchHandler, barrier);
            createdConsumers[i] = batchConsumer;
        }

        consumerRepository.unmarkConsumersAsEndOfChain(barrierConsumers);
        return new ConsumerGroup<T>(this, createdConsumers);
    }

    private void startConsumers()
    {
        for (ConsumerInfo<T> consumerInfo : consumerRepository)
        {
            executor.execute(consumerInfo.getConsumer());
        }
    }

}
