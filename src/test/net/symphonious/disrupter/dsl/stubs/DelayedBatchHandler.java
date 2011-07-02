package net.symphonious.disrupter.dsl.stubs;

import com.lmax.disruptor.BatchHandler;

import java.util.concurrent.atomic.AtomicBoolean;

public class DelayedBatchHandler implements BatchHandler<TestEntry>
{
    private AtomicBoolean readyToProcessEvent = new AtomicBoolean(false);

    public void onAvailable(final TestEntry entry) throws Exception
    {
        waitForAndSetFlag(false);
    }

    public void onEndOfBatch() throws Exception
    {
    }

    public void onCompletion()
    {
    }

    public void processEvent()
    {
        waitForAndSetFlag(true);
    }

    private void waitForAndSetFlag(final boolean newValue)
    {
        while (!readyToProcessEvent.compareAndSet(!newValue, newValue))
        {
            Thread.yield();
        }
    }
}
