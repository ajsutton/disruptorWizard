package net.symphonious.disrupter.dsl.stubs;

import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.Executor;

public class TestExecutor implements Executor
{
    private Collection<Thread> threads = new ArrayList<Thread>();

    public void execute(final Runnable command)
    {
        Thread t = new Thread(command);
        t.setName(command.toString());
        threads.add(t);
        t.start();
    }

    public void stopAll()
    {
        System.out.println("Beginning attempt to stop threads. Threads from this test: " + threads.size() + ". Total threads: " + Thread.activeCount());
        for (Thread thread : threads)
        {
            if (thread.isAlive())
            {
                try
                {
                    System.out.println("Joining thread: " + thread);
                    thread.join(5000);
                    System.out.println("Joined thread.");
                }
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
            Assert.assertFalse(thread.isAlive());
        }
        System.out.println("Finished attempt to stop threads.");
        threads.clear();
        Assert.assertEquals("Should only be one thread running.", 1, Thread.activeCount());
    }
}
