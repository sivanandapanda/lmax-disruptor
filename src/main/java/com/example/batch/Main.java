package com.example.batch;

import java.time.LocalTime;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.dsl.Disruptor;

public class Main {

   static class Event {

      int id;
   }

   static class Producer implements Runnable {

      private final RingBuffer<Event> ringbuffer;

      public Producer(RingBuffer<Event> rb) {
         ringbuffer = rb;
      }

      @Override
      public void run() {
         long next = 0L;
         int id = 0;
         while (true) {
            try {
               next = ringbuffer.next();
               Event e = ringbuffer.get(next);
               e.id = id++;
            } finally {
               ringbuffer.publish(next);
            }
         }
      }
   }

   static class Consumer {

      private final ExecutorService exec;
      private final Disruptor<Event> disruptor;
      private final RingBuffer<Event> ringbuffer;
      private final SequenceBarrier seqbar;
      private BatchEventProcessor<Event> processor;

      public Consumer() {
         exec = Executors.newCachedThreadPool();
         disruptor = new Disruptor<>(() -> new Event(), 1024, Executors.defaultThreadFactory());
         ringbuffer = disruptor.start();
         seqbar = ringbuffer.newBarrier();

         processor = new BatchEventProcessor<>(ringbuffer, seqbar, new Handler());
         ringbuffer.addGatingSequences(processor.getSequence());

         Producer producer = new Producer(ringbuffer);
         exec.submit(producer);
      }
   }

   static class Handler implements EventHandler<Event> {

      @Override
      public void onEvent(Event event, long sequence, boolean endOfBatch) throws Exception {
         System.out.println("Handling event " + event.id);
      }

   }

   public static void main(String[] args) {
      Consumer c = new Consumer();
      c.processor.run();
   }
}