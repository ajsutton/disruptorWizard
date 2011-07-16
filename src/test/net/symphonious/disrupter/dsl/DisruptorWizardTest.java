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
import net.symphonious.disrupter.dsl.stubs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;

@SuppressWarnings({"ThrowableResultOfMethodCallIgnored"})
public class DisruptorWizardTest
{

    private static final int TIMEOUT_IN_SECONDS = 2;
    private DisruptorWizard<TestEntry> disruptorWizard;
    private TestExecutor executor;
    private Collection<DelayedBatchHandler> delayedBatchHandlers = new ArrayList<DelayedBatchHandler>();

    @Before
    public void setUp() throws Exception
    {
        createDisruptor();
    }

    @After
    public void tearDown() throws Exception
    {
        for (DelayedBatchHandler delayedBatchHandler : delayedBatchHandlers)
        {
            delayedBatchHandler.stopWaiting();
        }
        disruptorWizard.halt();
        executor.stopAll();
    }

    @Test
    @SuppressWarnings({"unchecked"})
    public void shouldCreateConsumerGroupForFirstConsumers() throws Exception
    {
        executor.ignoreExecutions();
        final BatchHandler<TestEntry> batchHandler1 = new DoNothingBatchHandler();
        BatchHandler<TestEntry> batchHandler2 = new DoNothingBatchHandler();

        final ConsumerGroup consumerGroup = disruptorWizard.consumeWith(batchHandler1, batchHandler2);
        disruptorWizard.getProducerBarrier();

        assertNotNull(consumerGroup);
        assertThat(executor.getExecutionCount(), equalTo(2));
    }

    @Test
    public void shouldMakeEntriesAvailableToFirstHandlersImmediately() throws Exception
    {
        CountDownLatch countDownLatch = new CountDownLatch(2);
        BatchHandler<TestEntry> batchHandler2 = new BatchHandlerStub(countDownLatch);

        disruptorWizard.consumeWith(createDelayedBatchHandler(), batchHandler2);

        ProducerBarrier<TestEntry> producerBarrier = disruptorWizard.getProducerBarrier();
        produceEntry(producerBarrier);
        produceEntry(producerBarrier);

        assertTrue("Batch handler did not receive entries.", countDownLatch.await(TIMEOUT_IN_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    public void shouldWaitUntilAllFirstConsumersProcessEventBeforeMakingItAvailableToDependentConsumers() throws Exception
    {
        DelayedBatchHandler batchHandler1 = createDelayedBatchHandler();

        CountDownLatch countDownLatch = new CountDownLatch(2);
        BatchHandler<TestEntry> batchHandler2 = new BatchHandlerStub(countDownLatch);

        disruptorWizard.consumeWith(batchHandler1).then(batchHandler2);

        ProducerBarrier<TestEntry> producerBarrier = disruptorWizard.getProducerBarrier();
        produceEntry(producerBarrier);
        produceEntry(producerBarrier);

        assertThat(countDownLatch.getCount(), equalTo(2L));

        batchHandler1.processEvent();
        batchHandler1.processEvent();
        assertTrue("Batch handler did not receive entries.", countDownLatch.await(TIMEOUT_IN_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    public void shouldAllowSpecifyingSpecificConsumersToWaitFor() throws Exception
    {
        DelayedBatchHandler handler1 = createDelayedBatchHandler();
        DelayedBatchHandler handler2 = createDelayedBatchHandler();

        CountDownLatch countDownLatch = new CountDownLatch(2);
        BatchHandler<TestEntry> handlerWithBarrier = new BatchHandlerStub(countDownLatch);

        disruptorWizard.consumeWith(handler1, handler2);
        disruptorWizard.after(handler1, handler2).consumeWith(handlerWithBarrier);


        ProducerBarrier<TestEntry> producerBarrier = disruptorWizard.getProducerBarrier();
        produceEntry(producerBarrier);
        produceEntry(producerBarrier);

        assertThat(countDownLatch.getCount(), equalTo(2L));

        handler1.processEvent();
        handler2.processEvent();

        assertThat(countDownLatch.getCount(), equalTo(2L));

        handler2.processEvent();
        handler1.processEvent();
        assertTrue("Batch handler did not receive entries.", countDownLatch.await(TIMEOUT_IN_SECONDS, TimeUnit.SECONDS));
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldTrackBatchHandlersByIdentityNotEquality() throws Exception
    {
        EvilEqualsBatchHandler handler1 = new EvilEqualsBatchHandler();
        EvilEqualsBatchHandler handler2 = new EvilEqualsBatchHandler();

        disruptorWizard.consumeWith(handler1);
        disruptorWizard.after(handler2); // handler2.equals(handler1) but it hasn't yet been registered so should throw exception.
    }

    @Test
    public void shouldSupportSpecifyingADefaultExceptionHandlerForConsumers() throws Exception
    {
        AtomicReference<Exception> eventHandled = new AtomicReference<Exception>();
        ExceptionHandler exceptionHandler = new TestExceptionHandler(eventHandled);
        RuntimeException testException = new RuntimeException();
        ExceptionThrowingBatchHandler handler = new ExceptionThrowingBatchHandler(testException);


        disruptorWizard.handleExceptionsWith(exceptionHandler);
        disruptorWizard.consumeWith(handler);

        final ProducerBarrier<TestEntry> producerBarrier = disruptorWizard.getProducerBarrier();
        produceEntry(producerBarrier);

        final Exception actualException = waitFor(eventHandled);
        assertSame(testException, actualException);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfHandlerIsNotAlreadyConsuming() throws Exception
    {
        disruptorWizard.after(createDelayedBatchHandler()).consumeWith(createDelayedBatchHandler());
    }

    @Test
    public void shouldBlockProducerUntilAllConsumersHaveAdvanced() throws Exception
    {
        final DelayedBatchHandler handler1 = createDelayedBatchHandler();
        disruptorWizard.consumeWith(handler1);

        final ProducerBarrier<TestEntry> producerBarrier = disruptorWizard.getProducerBarrier();

        final TestProducer testProducer = new TestProducer(producerBarrier);
        try
        {
            executor.execute(testProducer);

            assertProducerReaches(testProducer, 4, true);

            handler1.processEvent();
            handler1.processEvent();
            handler1.processEvent();
            handler1.processEvent();
            handler1.processEvent();

            assertProducerReaches(testProducer, 5, false);
        }
        finally
        {
            testProducer.halt();
        }
    }

    @Test
    public void shouldGetBarrierForRegisteredConsumer() throws Exception
    {
        final DelayedBatchHandler batchHandler = createDelayedBatchHandler();
        disruptorWizard.consumeWith(batchHandler);

        ConsumerBarrier<TestEntry> barrier = disruptorWizard.getBarrierFor(batchHandler);

        assertThat(barrier.getCursor(), equalTo(-1L));
        final ProducerBarrier<TestEntry> producerBarrier = disruptorWizard.getProducerBarrier();
        produceEntry(producerBarrier);
        batchHandler.processEvent();

        assertConsumerReaches(barrier, 0L);
        batchHandler.processEvent();
    }

    @Test
    public void shouldReturnSameProducerAcrossMultipleCalls() throws Exception
    {
        executor.ignoreExecutions();
        disruptorWizard.consumeWith(new DoNothingBatchHandler());
        assertSame(disruptorWizard.getProducerBarrier(), disruptorWizard.getProducerBarrier());
    }

    @Test(expected = IllegalStateException.class)
    public void shouldThrowExceptionWhenAddingConsumersAfterTheProducerBarrierHasBeenCreated() throws Exception
    {
        executor.ignoreExecutions();
        disruptorWizard.consumeWith(new DoNothingBatchHandler());
        disruptorWizard.getProducerBarrier();
        disruptorWizard.consumeWith(new DoNothingBatchHandler());
    }

    private void assertConsumerReaches(final ConsumerBarrier<TestEntry> barrier, final long expectedCounter)
    {
        long loopStart = System.currentTimeMillis();
        while (barrier.getCursor() < expectedCounter && System.currentTimeMillis() - loopStart < 5000)
        {
            yield();
        }
        assertThat(barrier.getCursor(), equalTo(expectedCounter));
    }

    private void assertProducerReaches(final TestProducer testProducer, final int productionCount, boolean strict)
    {
        long loopStart = System.currentTimeMillis();
        while (testProducer.getProductionCount() < productionCount && System.currentTimeMillis() - loopStart < 5000)
        {
            yield();
        }
        if (strict)
        {
            assertThat(testProducer.getProductionCount(), equalTo(productionCount));
        }
        else
        {
            assertTrue("Producer reached expected count.", testProducer.getProductionCount() > productionCount);
        }
    }

    private void createDisruptor()
    {
        executor = new TestExecutor();
        createDisruptor(executor);
    }

    private void createDisruptor(final Executor executor)
    {
        disruptorWizard = new DisruptorWizard<TestEntry>(TestEntry.ENTRY_FACTORY, 4, executor, ClaimStrategy.Option.SINGLE_THREADED, WaitStrategy.Option.BLOCKING);
    }

    private TestEntry produceEntry(final ProducerBarrier<TestEntry> producerBarrier)
    {
        final TestEntry testEntry = producerBarrier.nextEntry();
        producerBarrier.commit(testEntry);
        return testEntry;
    }

    private Exception waitFor(final AtomicReference<Exception> reference)
    {
        while (reference.get() == null)
        {
            yield();
        }
        return reference.get();
    }

    private void yield()
    {
        Thread.yield();
//        try
//        {
//            Thread.sleep(500);
//        }
//        catch (InterruptedException e)
//        {
//            e.printStackTrace();
//        }
    }

    private DelayedBatchHandler createDelayedBatchHandler()
    {
        final DelayedBatchHandler delayedBatchHandler = new DelayedBatchHandler();
        delayedBatchHandlers.add(delayedBatchHandler);
        return delayedBatchHandler;
    }
}
