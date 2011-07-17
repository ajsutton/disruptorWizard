package net.symphonious.disrupter.dsl;

import com.lmax.disruptor.Consumer;
import com.lmax.disruptor.ConsumerBarrier;
import net.symphonious.disrupter.dsl.stubs.DoNothingBatchHandler;
import net.symphonious.disrupter.dsl.stubs.TestEntry;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class ConsumerRepositoryTest
{
    private ConsumerRepository<TestEntry> consumerRepository;
    private Consumer consumer1;
    private Consumer consumer2;
    private DoNothingBatchHandler handler1;
    private DoNothingBatchHandler handler2;
    private ConsumerBarrier<TestEntry> barrier1;
    private ConsumerBarrier<TestEntry> barrier2;

    @SuppressWarnings({"unchecked"})
    @Before
    public void setUp() throws Exception
    {
        consumerRepository = new ConsumerRepository<TestEntry>();
        consumer1 = mock(Consumer.class);
        consumer2 = mock(Consumer.class);
        handler1 = new DoNothingBatchHandler();
        handler2 = new DoNothingBatchHandler();

        barrier1 = mock(ConsumerBarrier.class);
        barrier2 = mock(ConsumerBarrier.class);
    }

    @Test
    public void shouldGetBarrierByHandler() throws Exception
    {
        consumerRepository.add(consumer1, handler1, barrier1);

        assertThat(consumerRepository.getBarrierFor(handler1), sameInstance(barrier1));
    }

    @Test
    public void shouldReturnNullForBarrierWhenHandlerIsNotRegistered() throws Exception
    {
        assertThat(consumerRepository.getBarrierFor(handler1), is(nullValue()));
    }

    @Test
    public void shouldGetLastConsumersInChain() throws Exception
    {
        consumerRepository.add(consumer1, handler1, barrier1);
        consumerRepository.add(consumer2, handler2, barrier2);

        consumerRepository.unmarkConsumersAsEndOfChain(consumer2);

        final Consumer[] lastConsumersInChain = consumerRepository.getLastConsumersInChain();
        assertThat(lastConsumersInChain.length, equalTo(1));
        assertThat(lastConsumersInChain[0], sameInstance(consumer1));
    }

    @Test
    public void shouldRetrieveConsumerForHandler() throws Exception
    {
        consumerRepository.add(consumer1, handler1, barrier1);

        assertThat(consumerRepository.getConsumerFor(handler1), sameInstance(consumer1));
    }

    @Test
    public void shouldReturnNullWhenHandlerIsNotRegistered() throws Exception
    {
        assertThat(consumerRepository.getConsumerFor(new DoNothingBatchHandler()), is(nullValue()));
    }

    @Test
    public void shouldIterateAllConsumers() throws Exception
    {
        consumerRepository.add(consumer1, handler1, barrier1);
        consumerRepository.add(consumer2, handler2, barrier2);


        boolean seen1 = false;
        boolean seen2 = false;
        for (ConsumerInfo<TestEntry> testEntryConsumerInfo : consumerRepository)
        {
            if (!seen1 && testEntryConsumerInfo.getConsumer() == consumer1 && testEntryConsumerInfo.getHandler() == handler1)
            {
                seen1 = true;
            }
            else if (!seen2 && testEntryConsumerInfo.getConsumer() == consumer2 && testEntryConsumerInfo.getHandler() == handler2)
            {
                seen2 = true;
            }
            else
            {
                fail("Unexpected consumer info: " + testEntryConsumerInfo);
            }
        }
        assertTrue("Included consumer 1", seen1);
        assertTrue("Included consumer 2", seen2);
    }
}
