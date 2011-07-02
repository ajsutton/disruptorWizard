package net.symphonious.disrupter.dsl.stubs;

import com.lmax.disruptor.BatchHandler;

public class ExceptionThrowingBatchHandler implements BatchHandler<TestEntry>
{
    private final RuntimeException testException;

    public ExceptionThrowingBatchHandler(final RuntimeException testException)
    {
        this.testException = testException;
    }

    public void onAvailable(final TestEntry entry) throws Exception
    {
        throw testException;
    }

    public void onEndOfBatch() throws Exception
    {
    }

    public void onCompletion()
    {
    }
}
