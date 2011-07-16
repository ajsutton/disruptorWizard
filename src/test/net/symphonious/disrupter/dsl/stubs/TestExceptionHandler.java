package net.symphonious.disrupter.dsl.stubs;

import com.lmax.disruptor.AbstractEntry;
import com.lmax.disruptor.ExceptionHandler;

import java.util.concurrent.atomic.AtomicReference;

public class TestExceptionHandler implements ExceptionHandler
{

    private final AtomicReference<Exception> exceptionHandled;

    public TestExceptionHandler(final AtomicReference<Exception> exceptionHandled)
    {
        this.exceptionHandled = exceptionHandled;
    }

    public void handle(final Exception ex, final AbstractEntry currentEntry)
    {
        exceptionHandled.set(ex);
    }
}
