package net.symphonious.disrupter.dsl;

import com.lmax.disruptor.AbstractEntry;
import com.lmax.disruptor.BatchHandler;
import com.lmax.disruptor.Consumer;

class ConsumerInfo<T extends AbstractEntry>
{
    private final Consumer consumer;
    private final BatchHandler<T> handler;

    private boolean endOfChain = true;

    ConsumerInfo(final Consumer consumer, final BatchHandler<T> handler)
    {
        this.consumer = consumer;
        this.handler = handler;
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

    public boolean isEndOfChain()
    {
        return endOfChain;
    }

    public void usedInBarrier()
    {
        endOfChain = false;
    }
}
