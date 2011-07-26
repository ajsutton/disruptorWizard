package net.symphonious.disrupter.dsl.stubs;

import com.lmax.disruptor.BatchHandler;

public class DoNothingBatchHandler implements BatchHandler<TestEntry>
{
    public void onAvailable(final TestEntry entry) throws Exception
    {
        Thread.sleep(1000);
    }

    public void onEndOfBatch() throws Exception
    {
    }
}
