package net.symphonious.disrupter.dsl;

import com.lmax.disruptor.AbstractEntry;
import com.lmax.disruptor.BatchHandler;
import com.lmax.disruptor.Consumer;
import com.lmax.disruptor.ConsumerBarrier;

class ConsumerInfo<T extends AbstractEntry>
{
    private final Consumer consumer;
    private final BatchHandler<T> handler;
    private final ConsumerBarrier<T> barrier;

    private boolean endOfChain = true;

    ConsumerInfo(final Consumer consumer, final BatchHandler<T> handler, final ConsumerBarrier<T> barrier)
    {
        this.consumer = consumer;
        this.handler = handler;
        this.barrier = barrier;
        this.endOfChain = true;
    }

    public Consumer getConsumer()
    {
        return consumer;
    }

    public BatchHandler<T> getHandler()
    {
        return handler;
    }

    public ConsumerBarrier<T> getBarrier()
    {
        return barrier;
    }

    public boolean isEndOfChain()
    {
        return endOfChain;
    }

    public void usedInBarrier()
    {
        endOfChain = false;
    }
}
