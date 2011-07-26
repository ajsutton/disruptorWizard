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
    private DisruptorWizard<StubEntry> disruptorWizard;
    private StubExecutor executor;
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
        final BatchHandler<StubEntry> batchHandler1 = new DoNothingBatchHandler();
        BatchHandler<StubEntry> batchHandler2 = new DoNothingBatchHandler();

        final ConsumerGroup consumerGroup = disruptorWizard.consumeWith(batchHandler1, batchHandler2);
        disruptorWizard.getProducerBarrier();

        assertNotNull(consumerGroup);
        assertThat(executor.getExecutionCount(), equalTo(2));
    }

    @Test
    public void shouldMakeEntriesAvailableToFirstHandlersImmediately() throws Exception
    {
        CountDownLatch countDownLatch = new CountDownLatch(2);
        BatchHandler<StubEntry> batchHandler2 = new BatchHandlerStub(countDownLatch);

        disruptorWizard.consumeWith(createDelayedBatchHandler(), batchHandler2);

        produceEntry();
        produceEntry();

        assertTrue("Batch handler did not receive entries.", countDownLatch.await(TIMEOUT_IN_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    public void shouldWaitUntilAllFirstConsumersProcessEventBeforeMakingItAvailableToDependentConsumers() throws Exception
    {
        DelayedBatchHandler batchHandler1 = createDelayedBatchHandler();

        CountDownLatch countDownLatch = new CountDownLatch(2);
        BatchHandler<StubEntry> batchHandler2 = new BatchHandlerStub(countDownLatch);

        disruptorWizard.consumeWith(batchHandler1).then(batchHandler2);

        produceEntry();
        produceEntry();

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
        BatchHandler<StubEntry> handlerWithBarrier = new BatchHandlerStub(countDownLatch);

        disruptorWizard.consumeWith(handler1, handler2);
        disruptorWizard.after(handler1, handler2).consumeWith(handlerWithBarrier);


        produceEntry();
        produceEntry();

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
        ExceptionHandler exceptionHandler = new StubExceptionHandler(eventHandled);
        RuntimeException testException = new RuntimeException();
        ExceptionThrowingBatchHandler handler = new ExceptionThrowingBatchHandler(testException);


        disruptorWizard.handleExceptionsWith(exceptionHandler);
        disruptorWizard.consumeWith(handler);

        produceEntry();

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

        final ProducerBarrier<StubEntry> producerBarrier = disruptorWizard.getProducerBarrier();

        final StubProducer stubProducer = new StubProducer(producerBarrier);
        try
        {
            executor.execute(stubProducer);

            assertProducerReaches(stubProducer, 4, true);

            handler1.processEvent();
            handler1.processEvent();
            handler1.processEvent();
            handler1.processEvent();
            handler1.processEvent();

            assertProducerReaches(stubProducer, 5, false);
        }
        finally
        {
            stubProducer.halt();
        }
    }

    @Test
    public void shouldBeAbleToOverrideTheExceptionHandlerForAConsumer() throws Exception
    {
        final DelayedBatchHandler batchHandler = createDelayedBatchHandler();

        final RuntimeException testException = new RuntimeException();
        final ExceptionThrowingBatchHandler batchHandler2 = new ExceptionThrowingBatchHandler(testException);
        disruptorWizard.consumeWith(batchHandler).then(batchHandler2);

        produceEntry();

        AtomicReference<Exception> reference = new AtomicReference<Exception>();
        StubExceptionHandler exceptionHandler = new StubExceptionHandler(reference);
        disruptorWizard.handleExceptionsFor(batchHandler2).with(exceptionHandler);
        batchHandler.processEvent();

        waitFor(reference);
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

    private void assertProducerReaches(final StubProducer stubProducer, final int productionCount, boolean strict)
    {
        long loopStart = System.currentTimeMillis();
        while (stubProducer.getProductionCount() < productionCount && System.currentTimeMillis() - loopStart < 5000)
        {
            yield();
        }
        if (strict)
        {
            assertThat(stubProducer.getProductionCount(), equalTo(productionCount));
        }
        else
        {
            assertTrue("Producer reached expected count.", stubProducer.getProductionCount() > productionCount);
        }
    }

    private void createDisruptor()
    {
        executor = new StubExecutor();
        createDisruptor(executor);
    }

    private void createDisruptor(final Executor executor)
    {
        disruptorWizard = new DisruptorWizard<StubEntry>(StubEntry.ENTRY_FACTORY, 4, executor, ClaimStrategy.Option.SINGLE_THREADED, WaitStrategy.Option.BLOCKING);
    }

    private StubEntry produceEntry()
    {
        final ProducerBarrier<StubEntry> producerBarrier = disruptorWizard.getProducerBarrier();
        final StubEntry stubEntry = producerBarrier.nextEntry();
        producerBarrier.commit(stubEntry);
        return stubEntry;
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
