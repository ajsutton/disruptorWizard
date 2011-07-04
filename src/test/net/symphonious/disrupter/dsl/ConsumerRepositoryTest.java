package net.symphonious.disrupter.dsl;

import com.lmax.disruptor.Consumer;
import net.symphonious.disrupter.dsl.stubs.DelayedBatchHandler;
import net.symphonious.disrupter.dsl.stubs.TestEntry;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

public class ConsumerRepositoryTest
{
    private ConsumerRepository<TestEntry> consumerRepository;

    @Before
    public void setUp() throws Exception
    {
        consumerRepository = new ConsumerRepository<TestEntry>();
    }

    @Test
    public void shouldGetLastConsumersInChain() throws Exception
    {
        final Consumer consumer1 = mock(Consumer.class);
        final Consumer consumer2 = mock(Consumer.class);
        final DelayedBatchHandler handler1 = new DelayedBatchHandler();
        final DelayedBatchHandler handler2 = new DelayedBatchHandler();

        consumerRepository.add(consumer1, handler1);
        consumerRepository.add(consumer2, handler2);

        consumerRepository.unmarkConsumersAsEndOfChain(consumer2);

        final Consumer[] lastConsumersInChain = consumerRepository.getLastConsumersInChain();
        assertThat(lastConsumersInChain.length, equalTo(1));
        assertThat(lastConsumersInChain[0], sameInstance(consumer1));
    }

    @Test
    public void shouldRetrieveConsumerForHandler() throws Exception
    {
        final Consumer consumer1 = mock(Consumer.class);
        final DelayedBatchHandler handler1 = new DelayedBatchHandler();

        consumerRepository.add(consumer1, handler1);

        assertThat(consumerRepository.getConsumerFor(handler1), sameInstance(consumer1));
    }

    @Test
    public void shouldReturnNullWhenHandlerIsNotRegistered() throws Exception
    {
        assertThat(consumerRepository.getConsumerFor(new DelayedBatchHandler()), nullValue());
    }

    @Test
    public void shouldIterateAllConsumers() throws Exception
    {
        final Consumer consumer1 = mock(Consumer.class);
        final Consumer consumer2 = mock(Consumer.class);
        final DelayedBatchHandler handler1 = new DelayedBatchHandler();
        final DelayedBatchHandler handler2 = new DelayedBatchHandler();

        consumerRepository.add(consumer1, handler1);
        consumerRepository.add(consumer2, handler2);


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
    }
}
