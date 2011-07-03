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

import com.lmax.disruptor.BatchConsumer;
import com.lmax.disruptor.BatchHandler;
import com.lmax.disruptor.ExceptionHandler;
import com.lmax.disruptor.ProducerBarrier;
import net.symphonious.disrupter.dsl.stubs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class DisruptorWizardTest
{

    private static final int TIMEOUT_IN_SECONDS = 2;
    private DisruptorWizard<TestEntry> disruptorWizard;

    @Before
    public void setUp() throws Exception
    {
        createDisruptor();
    }

    @After
    public void tearDown() throws Exception
    {
        disruptorWizard.halt();
    }

    @Test
    @SuppressWarnings({"unchecked"})
    public void shouldCreateConsumerGroupForFirstConsumers() throws Exception
    {
        final BatchHandler<TestEntry> batchHandler1 = mock(BatchHandler.class);
        BatchHandler<TestEntry> batchHandler2 = mock(BatchHandler.class);
        Executor executor = mock(Executor.class);
        createDisruptor(executor);

        final ConsumerGroup consumerGroup = disruptorWizard.consumeWith(batchHandler1, batchHandler2);

        assertNotNull(consumerGroup);
        verify(executor, times(2)).execute(any(BatchConsumer.class));
    }

    @Test
    public void shouldMakeEntriesAvailableToFirstHandlersImmediately() throws Exception
    {
        CountDownLatch countDownLatch = new CountDownLatch(2);
        BatchHandler<TestEntry> batchHandler2 = new BatchHandlerStub(countDownLatch);

        disruptorWizard.consumeWith(new DelayedBatchHandler(), batchHandler2);

        ProducerBarrier<TestEntry> producerBarrier = disruptorWizard.createProducerBarrier();
        produceEntry(producerBarrier);
        produceEntry(producerBarrier);

        assertTrue("Batch handler did not receive entries.", countDownLatch.await(TIMEOUT_IN_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    public void shouldWaitUntilAllFirstConsumersProcessEventBeforeMakingItAvailableToDependentConsumers() throws Exception
    {
        createDisruptor();

        DelayedBatchHandler batchHandler1 = new DelayedBatchHandler();

        CountDownLatch countDownLatch = new CountDownLatch(2);
        BatchHandler<TestEntry> batchHandler2 = new BatchHandlerStub(countDownLatch);

        disruptorWizard.consumeWith(batchHandler1).then(batchHandler2);

        ProducerBarrier<TestEntry> producerBarrier = disruptorWizard.createProducerBarrier();
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
        createDisruptor();

        DelayedBatchHandler handler1 = new DelayedBatchHandler();
        DelayedBatchHandler handler2 = new DelayedBatchHandler();

        CountDownLatch countDownLatch = new CountDownLatch(2);
        BatchHandler<TestEntry> handlerWithBarrier = new BatchHandlerStub(countDownLatch);

        disruptorWizard.consumeWith(handler1, handler2);
        disruptorWizard.after(handler1, handler2).consumeWith(handlerWithBarrier);


        ProducerBarrier<TestEntry> producerBarrier = disruptorWizard.createProducerBarrier();
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
        createDisruptor();
        EvilEqualsBatchHandler handler1 = new EvilEqualsBatchHandler();
        EvilEqualsBatchHandler handler2 = new EvilEqualsBatchHandler();

        disruptorWizard.consumeWith(handler1);
        disruptorWizard.after(handler2); // handler2.equals(handler1) but it hasn't yet been registered so should throw exception.
    }

    @Test
    public void shouldSupportSpecifyingADefaultExceptionHandlerForConsumers() throws Exception
    {
        createDisruptor();
        ExceptionHandler exceptionHandler = mock(ExceptionHandler.class);
        RuntimeException testException = new RuntimeException();
        ExceptionThrowingBatchHandler handler = new ExceptionThrowingBatchHandler(testException);

        disruptorWizard.handleExceptionsWith(exceptionHandler);
        disruptorWizard.consumeWith(handler);

        final TestEntry entry = produceEntry(disruptorWizard.createProducerBarrier());

        verify(exceptionHandler).handle(testException, entry);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldThrowExceptionIfHandlerIsNotAlreadyConsuming() throws Exception
    {
        disruptorWizard.after(new DelayedBatchHandler()).consumeWith(new DelayedBatchHandler());
    }

    @Test
    public void shouldBlockProducerUntilAllConsumersHaveAdvanced() throws Exception
    {
        createDisruptor();
        final DelayedBatchHandler handler1 = new DelayedBatchHandler();
        final BatchHandler<TestEntry> handler2 = new BatchHandlerStub(new CountDownLatch(4));
        disruptorWizard.consumeWith(handler1).then(handler2);

        final ProducerBarrier<TestEntry> producerBarrier = disruptorWizard.createProducerBarrier();

        final TestProducer testProducer = new TestProducer(producerBarrier);
        Executors.newSingleThreadExecutor().execute(testProducer);

        assertProducerReaches(testProducer, 3);

        handler1.processEvent();

        assertProducerReaches(testProducer, 4);
    }

    private void assertProducerReaches(final TestProducer testProducer, final int productionCount)
    {
        long loopStart = System.currentTimeMillis();
        while (testProducer.getProductionCount() < productionCount && System.currentTimeMillis() - loopStart < 3000)
        {
            Thread.yield();
        }
        assertThat(testProducer.getProductionCount(), equalTo(productionCount));
    }

    private void createDisruptor()
    {
        createDisruptor(Executors.newCachedThreadPool());
    }

    private void createDisruptor(final Executor executor)
    {
        disruptorWizard = new DisruptorWizard<TestEntry>(TestEntry.ENTRY_FACTORY, 4, executor);
    }

    private TestEntry produceEntry(final ProducerBarrier<TestEntry> producerBarrier)
    {
        final TestEntry testEntry = producerBarrier.nextEntry();
        producerBarrier.commit(testEntry);
        return testEntry;
    }
}
