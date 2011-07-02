package net.symphonious.disrupter.dsl.stubs;

import com.lmax.disruptor.BatchHandler;

import java.util.concurrent.CountDownLatch;

public class BatchHandlerStub implements BatchHandler<TestEntry>
{
    private final CountDownLatch countDownLatch;

    public BatchHandlerStub(final CountDownLatch countDownLatch)
    {
        this.countDownLatch = countDownLatch;
    }

    public void onAvailable(final TestEntry entry) throws Exception
    {
        countDownLatch.countDown();
    }

    public void onEndOfBatch() throws Exception
    {
    }

    public void onCompletion()
    {
    }
}
