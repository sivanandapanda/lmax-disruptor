package com.example.simple;

import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;

public class Main {

    public static void main(String[] args) {
        DaemonThreadFactory threadFactory = DaemonThreadFactory.INSTANCE;
        BusySpinWaitStrategy waitStrategy = new BusySpinWaitStrategy();

        EventFactory<ValueEvent<String>> EVENT_FACTORY = ValueEvent::new;

        Disruptor<ValueEvent<String>> disruptor = new Disruptor<>(EVENT_FACTORY, 16, threadFactory, ProducerType.SINGLE, waitStrategy);

        disruptor.handleEventsWith(new ValueConsumer<String>().getEventHandler());


        RingBuffer<ValueEvent<String>> ringBuffer = disruptor.start();


        for (int eventCount = 0; eventCount < 32; eventCount++) {
            long sequenceId = ringBuffer.next();
            ValueEvent<String> valueEvent = ringBuffer.get(sequenceId);
            valueEvent.setValue(String.valueOf(eventCount));
            ringBuffer.publish(sequenceId);
        }
    }


    private static class ValueConsumer<T> {
        private EventHandler<ValueEvent<T>>[] getEventHandler() {
            EventHandler<ValueEvent<T>> eventHandler = (event, sequence, endOfBatch) -> print(event.getValue(), sequence);
            return new EventHandler[] { eventHandler };
        }

        private void print(T value, long sequenceId) {
            System.out.println("Id is " + value + " sequence id that was used is " + sequenceId);
        }
    }

    private static class ValueEvent<T> {
        private T value;

        private T getValue() {
            return value;
        }

        private void setValue(T value) {
            this.value = value;
        }
    }
}