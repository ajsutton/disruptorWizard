package net.symphonious.disrupter.dsl.stubs;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Executor;

public class TestExecutor implements Executor
{
    private Collection<Thread> threads = new ArrayList<Thread>();
    private boolean ignoreExecutions = false;
    private int executionCount = 0;

    public void execute(final Runnable command)
    {
        executionCount++;
        if (!ignoreExecutions)
        {
            Thread t = new Thread(command);
            t.setName(command.toString());
            threads.add(t);
            t.start();
        }
    }

    public void stopAll()
    {
        for (Thread thread : threads)
        {
            if (thread.isAlive())
            {
                try
                {
                    thread.join(5000);
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
            Assert.assertFalse("Failed to stop thread: " + thread, thread.isAlive());
        }
        threads.clear();
    }

    public void ignoreExecutions()
    {
        ignoreExecutions = true;
    }

    public int getExecutionCount()
    {
        return executionCount;
    }
}
