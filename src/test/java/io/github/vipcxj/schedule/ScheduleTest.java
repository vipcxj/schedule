package io.github.vipcxj.schedule;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class ScheduleTest {

    private void shuffle(long[] array) {
        Random rnd = new Random();
        for (int i = array.length; i>1; i--) {
            int j = rnd.nextInt(i);
            if (j != i - 1) {
                long tmp = array[i - 1];
                array[i - 1] = array[j];
                array[j] = tmp;
            }
        }
    }

    @Test
    void testAddEvent() throws InterruptedException {
        long[] deadlines = LongStream.range(1, 1001).map(i -> i * 5).toArray();
        shuffle(deadlines);
        Schedule schedule = Schedule.instance();
        AtomicInteger index = new AtomicInteger();
        AtomicLong error = new AtomicLong();
        long[] results = new long[1000];
        long now = System.nanoTime();
        IntStream.range(0, deadlines.length).parallel().forEach(i -> {
            long timeout = deadlines[i];
            long deadline = timeout * 1000 * 1000 + System.nanoTime();
            deadlines[i] = deadline;
            schedule.addEvent(timeout, TimeUnit.MILLISECONDS, () -> {
                error.getAndAdd(Math.abs(System.nanoTime() - deadline));
                results[index.getAndIncrement()] = deadline;
            });
        });
        System.out.println("Use time: " + (System.nanoTime() - now) / 1000.0 / 1000.0);
        Thread.sleep(5200);
        Arrays.sort(deadlines);
        Assertions.assertArrayEquals(deadlines, results);
        long errorMs = error.get() / 1000 / 1000;
        System.out.println("Sum error: " + errorMs + " / Mean error: " + errorMs / 1000.0);
    }

    @Test
    void testRemoveEvent() throws InterruptedException {
        Random random = new Random();
        long[] timeouts = new long[1000];
        for (int i = 0; i < timeouts.length; ++i) {
            timeouts[i] = 100 + random.nextInt(1000);
        }
        AtomicInteger index1 = new AtomicInteger();
        AtomicInteger index2 = new AtomicInteger();
        AtomicLong error = new AtomicLong();
        IntStream.range(0, timeouts.length).forEach(i -> {
            long timeout = timeouts[i] * 1000 * 1000;
            EventHandle handle = Schedule.instance().addEvent(timeout, index1::getAndIncrement);
            long deadline = timeout / 2 + System.nanoTime();
            Schedule.instance().addEvent(timeout / 2, () -> {
                // System.out.println("removing");
                index2.getAndIncrement();
                error.getAndAdd(Math.abs(System.nanoTime() - deadline));
                boolean removed = handle.remove();
                Assertions.assertTrue(removed);
            });
        });
        Thread.sleep(1200);
        Assertions.assertEquals(0, index1.get());
        Assertions.assertEquals(1000, index2.get());
        long errorMs = error.get() / 1000 / 1000;
        System.out.println("Sum error: " + errorMs + " / Mean error: " + errorMs / 1000.0);
    }
}
