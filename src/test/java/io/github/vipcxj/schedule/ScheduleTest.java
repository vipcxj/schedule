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
    void test() throws InterruptedException {
        long[] deadlines = LongStream.range(1, 1001).map(i -> i * 3).toArray();
        shuffle(deadlines);
        Schedule schedule = new Schedule();
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
        Thread.sleep(3200);
        Arrays.sort(deadlines);
        Assertions.assertArrayEquals(deadlines, results);
        long errorMs = error.get() / 1000 / 1000;
        System.out.println("Sum error: " + errorMs + " / Mean error: " + errorMs / 1000.0);
    }
}
