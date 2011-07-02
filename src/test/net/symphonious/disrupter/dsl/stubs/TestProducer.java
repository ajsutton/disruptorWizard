package net.symphonious.disrupter.dsl.stubs;

import com.lmax.disruptor.ProducerBarrier;

public class TestProducer implements Runnable
{

    private volatile boolean running = true;

    private volatile int productionCount = 0;
    private final ProducerBarrier<TestEntry> producerBarrier;

    public TestProducer(ProducerBarrier<TestEntry> producerBarrier)
    {
        this.producerBarrier = producerBarrier;
    }

    public void run()
    {
        while (running)
        {
            final TestEntry entry = producerBarrier.nextEntry();
            producerBarrier.commit(entry);
            productionCount++;
        }
    }

    public int getProductionCount()
    {
        return productionCount;
    }

    public void halt()
    {
        running = false;
    }
}
