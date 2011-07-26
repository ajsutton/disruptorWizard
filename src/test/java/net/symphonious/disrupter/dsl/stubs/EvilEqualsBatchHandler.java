package net.symphonious.disrupter.dsl.stubs;

import com.lmax.disruptor.BatchHandler;

public class EvilEqualsBatchHandler implements BatchHandler<StubEntry>
{
    public void onAvailable(final StubEntry entry) throws Exception
    {
    }

    public void onEndOfBatch() throws Exception
    {
    }

    @SuppressWarnings({"EqualsWhichDoesntCheckParameterClass"})
    public boolean equals(Object o)
    {
        return true;
    }

    public int hashCode()
    {
        return 1;
    }
}
