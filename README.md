Disruptor Wizard
================

The disruptor wizard is a simple DSL to make setting up the [LMAX Disruptor](http://code.google.com/p/disruptor/) simpler.
The LMAX disruptor itself is an extremely high performance, low latency method of passing data between producers and consumers
in a lock-free manner. It was developed by [LMAX](http://www.lmaxtrader.co.uk) as part of their financial trading platform.

Examples
--------
A simple consumer chain where A must process events before B which must be before C which in turn must be before D.

    DisruptorWizard dw = new DisruptorWizard(ENTRY_FACTORY, ringBufferSize, executor);
    dw.consumeWith(A).then(B).then(C).then(D);

A diamond shape dependency where A and B process any event. C depends on A, D depends on B and E depends on all previous tasks.

    dw.consumeWith(A, B);
    dw.after(A).consumeWith(C);
    dw.after(B).consumeWIth(D);
    dw.after(C, D).consumeWith(E);