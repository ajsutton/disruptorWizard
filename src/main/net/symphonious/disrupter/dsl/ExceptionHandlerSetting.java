package net.symphonious.disrupter.dsl;

import com.lmax.disruptor.AbstractEntry;
import com.lmax.disruptor.BatchConsumer;
import com.lmax.disruptor.BatchHandler;
import com.lmax.disruptor.ExceptionHandler;

public class ExceptionHandlerSetting<T extends AbstractEntry>
{
    private final BatchHandler<T> batchHandler;
    private final ConsumerRepository<T> consumerRepository;

    ExceptionHandlerSetting(final BatchHandler<T> batchHandler, ConsumerRepository<T> consumerRepository)
    {
        this.batchHandler = batchHandler;
        this.consumerRepository = consumerRepository;
    }

    public void with(ExceptionHandler exceptionHandler)
    {
        ((BatchConsumer)consumerRepository.getConsumerFor(batchHandler)).setExceptionHandler(exceptionHandler);
        consumerRepository.getBarrierFor(batchHandler).alert();
    }
}
