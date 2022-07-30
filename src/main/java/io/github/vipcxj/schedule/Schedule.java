package io.github.vipcxj.schedule;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * High-performance schedule manager
 */
public class Schedule implements Closeable {

    private volatile Event head;
    private static final AtomicReferenceFieldUpdater<Schedule, Event> HEAD = AtomicReferenceFieldUpdater.newUpdater(Schedule.class, Event.class, "head");
    private static final Event BUSY = new Event(0, null, null);
    private final Thread thread;
    private volatile boolean parking;
    private static final AtomicReference<Schedule> INSTANCE = new AtomicReference<>();

    /**
     * Get the singleton instance.
     * @return singleton instance
     */
    public static Schedule instance() {
        Schedule schedule = INSTANCE.get();
        if (schedule == null) {
            schedule = new Schedule();
            if (!INSTANCE.compareAndSet(null, schedule)) {
                schedule.close();
                schedule = INSTANCE.get();
            }
        }
        return schedule;
    }

    public Schedule() {
        thread = new Thread(this::run);
        thread.start();
    }

    private synchronized void signal() {
        if (parking) {
            notify();
        }
    }

    private synchronized void waitForever() throws InterruptedException {
        this.head = null;
        this.parking = true;
        try {
            this.wait();
        } finally {
            this.parking = false;
        }
    }

    private synchronized void waitFor(long ns) throws InterruptedException {
        long ms = ns / 1000 / 1000;
        this.parking = true;
        try {
            this.wait(ms, (int) (ns - (ms * 1000 * 1000)));
        } finally {
            this.parking = false;
        }
    }

    @Override
    public void close() {
        thread.interrupt();
    }

    private void run() {
        try {
            while (true) {
                Event head = this.head;
                if (head != null) {
                    long now = System.nanoTime();
                    if (head.deadline <= now) {
                        Event next = head.next;
                        if (Event.NEXT.weakCompareAndSet(head, next, BUSY)) {
                            if (head == this.head && HEAD.weakCompareAndSet(this, head, next)) {
                                head.next = null;
                                head.run();
                            } else {
                                head.next = next;
                            }
                        } else if (next == BUSY) {
                            throw new IllegalStateException("This is impossible.");
                        }
                    } else if (head.deadline - now > 1000 * 100){
                        waitFor(head.deadline - now);
                    } else {
                        Thread.yield();
                    }
                } else {
                    // promise wait and addEvent will not invoked at the same time.
                    if (HEAD.weakCompareAndSet(this, null, BUSY)) {
                        waitForever();
                    }
                }
            }
        } catch (InterruptedException ignored) { }
    }

    static class Event {
        private final long deadline;
        private final Object callbacks;
        private volatile Event next;
        private static final AtomicReferenceFieldUpdater<Event, Event> NEXT = AtomicReferenceFieldUpdater.newUpdater(Event.class, Event.class, "next");
        Event(long deadline, Object callbacks, Event next) {
            this.deadline = deadline;
            this.callbacks = callbacks;
            this.next = next;
        }

        public void run() {
            if (callbacks instanceof Runnable) {
                try {
                    ((Runnable) callbacks).run();
                } catch (Throwable t) {
                    t.printStackTrace();
                }
            } else {
                for (Runnable callback : (Runnable[]) callbacks) {
                    try {
                        callback.run();
                    } catch (Throwable t) {
                        t.printStackTrace();
                    }
                }

            }
        }
    }

    private Event addCallback(Event event, Runnable callback) {
        Object callbacks = event.callbacks;
        Runnable[] newCallbacks;
        if (callbacks instanceof Runnable) {
            newCallbacks = new Runnable[2];
            newCallbacks[0] = (Runnable) callbacks;
            newCallbacks[1] = callback;
        } else {
            Runnable[] oldCallbacks = (Runnable[]) callbacks;
            newCallbacks = new Runnable[oldCallbacks.length + 1];
            System.arraycopy(oldCallbacks, 0, newCallbacks, 0, oldCallbacks.length);
            newCallbacks[oldCallbacks.length] = callback;
        }
        return new Event(event.deadline, newCallbacks, event.next);
    }

    /**
     * schedule the callback
     * @param time the time when the callback invoked.
     * @param unit the time unit.
     * @param callback the callback.
     */
    public void addEvent(long time, TimeUnit unit, Runnable callback) {
        addEvent(unit.toNanos(time), callback);
    }

    /**
     * schedule the callback
     * @param nanoTime the time in nanoseconds when the callback invoked.
     * @param callback the callback.
     */
    public void addEvent(long nanoTime, Runnable callback) {
        long deadline = System.nanoTime() + nanoTime;
        while (true) {
            Event head = this.head;
            if (head == null) {
                if (HEAD.weakCompareAndSet(this, null, new Event(deadline, callback, null))) {
                    signal();
                    return;
                }
            } else if (head != BUSY) {
                if (head.deadline > deadline && HEAD.weakCompareAndSet(this, head, new Event(deadline, callback, head))) {
                    signal();
                    return;
                } else if (head.deadline == deadline && HEAD.weakCompareAndSet(this, head, addCallback(head, callback))) {
                    return;
                } else if (head.deadline < deadline) {
                    Event node = head;
                    Event next = node.next;
                    if (next != BUSY) {
                        while (next != null && next.deadline < deadline) {
                            node = next;
                            next = next.next;
                        }
                    }
                    Event newEvent;
                    if (next != BUSY) {
                        if (next == null) {
                            newEvent = new Event(deadline, callback, null);
                            if (Event.NEXT.weakCompareAndSet(node, null, newEvent)) {
                                return;
                            }
                        } else if (next.deadline == deadline) {
                            newEvent = addCallback(next, callback);
                            if (Event.NEXT.weakCompareAndSet(node, next, newEvent)) {
                                return;
                            }
                        } else {
                            newEvent = new Event(deadline, callback, next);
                            if (Event.NEXT.weakCompareAndSet(node, next, newEvent)) {
                                return;
                            }
                        }
                    }
                }
            }
        }
    }
}
