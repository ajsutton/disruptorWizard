package net.symphonious.disrupter.dsl.stubs;

import com.lmax.disruptor.BatchHandler;

import java.util.concurrent.atomic.AtomicBoolean;

public class ExceptionThrowingBatchHandler implements BatchHandler<TestEntry>
{
    private final AtomicBoolean eventHandled;
    private final RuntimeException testException;

    public ExceptionThrowingBatchHandler(final AtomicBoolean eventHandled, final RuntimeException testException)
    {
        this.eventHandled = eventHandled;
        this.testException = testException;
    }

    public void onAvailable(final TestEntry entry) throws Exception
    {
        eventHandled.set(true);
        throw testException;
    }

    public void onEndOfBatch() throws Exception
    {
    }
}
