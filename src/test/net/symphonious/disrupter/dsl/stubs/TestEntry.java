package net.symphonious.disrupter.dsl.stubs;

import com.lmax.disruptor.AbstractEntry;
import com.lmax.disruptor.EntryFactory;

public class TestEntry extends AbstractEntry
{
    public static final EntryFactory<TestEntry> ENTRY_FACTORY = new EntryFactory<TestEntry>()
    {
        public TestEntry create()
        {
            return new TestEntry();
        }
    };
}
