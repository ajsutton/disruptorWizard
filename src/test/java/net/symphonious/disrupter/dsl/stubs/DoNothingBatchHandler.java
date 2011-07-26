package net.symphonious.disrupter.dsl.stubs;

import com.lmax.disruptor.BatchHandler;

public class DoNothingBatchHandler implements BatchHandler<StubEntry>
{
    public void onAvailable(final StubEntry entry) throws Exception
    {
        Thread.sleep(1000);
    }

    public void onEndOfBatch() throws Exception
    {
    }
}
