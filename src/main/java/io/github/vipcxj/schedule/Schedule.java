package io.github.vipcxj.schedule;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

/**
 * High-performance schedule manager
 */
public class Schedule implements Closeable {

    private final Event head = new Event(0, null, TAIL, null);
    private static final Event TAIL = new Event(0, null, null, null);
    private static final Event BUSY = new Event(0, null, null, null);
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
        this.head.next = TAIL;
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
                Event first = this.head.next;
                assert first != null;
                if (first != TAIL) {
                    long now = System.nanoTime();
                    if (first.deadline <= now) {
                        Event prev = first.tryLockPrev();
                        if (prev != null) {
                            Event next = first.next;
                            // The next is null if and only if first is removed.
                            // We always lock first.prev before remove it.
                            // Since we have successfully locked the first.prev,
                            // we can make sure next is not null.
                            assert next != null;
                            if (next == TAIL) {
                                if (head.weakCasNext(first, TAIL)) {
                                    first.prev = first.next = null;
                                    first.run();
                                } else {
                                    first.prev = prev;
                                }
                            } else {
                                Event nextPrev = next.tryLockPrev();
                                if (nextPrev != null) {
                                    if (nextPrev != first) {
                                        next.prev = nextPrev;
                                        first.prev = prev;
                                        continue;
                                    }
                                    if (first == this.head.next && head.weakCasNext(first, next)) {
                                        next.prev = head;
                                        first.prev = first.next = null;
                                        first.run();
                                    } else {
                                        next.prev = nextPrev;
                                        first.prev = prev;
                                    }
                                } else {
                                    first.prev = prev;
                                }
                            }
                        }
                    } else if (first.deadline - now > 1000 * 100){
                        waitFor(first.deadline - now);
                    } else {
                        Thread.yield();
                    }
                } else if (first == BUSY) {
                    Thread.yield();
                } else {
                    if (this.head.weakCasNext(TAIL, BUSY)) {
                        waitForever();
                    }
                }
            }
        } catch (InterruptedException ignored) { }
    }

    static class Event implements EventHandle {
        private final long deadline;
        private final Runnable callback;
        private volatile Event next;
        private static final AtomicReferenceFieldUpdater<Event, Event> NEXT
                = AtomicReferenceFieldUpdater.newUpdater(Event.class, Event.class, "next");
        private volatile Event prev;
        private static final AtomicReferenceFieldUpdater<Event, Event> PREV
                = AtomicReferenceFieldUpdater.newUpdater(Event.class, Event.class, "prev");
        Event(long deadline, Runnable callback, Event next, Event prev) {
            this.deadline = deadline;
            this.callback = callback;
            this.next = next;
            this.prev = prev;
        }

        boolean weakCasNext(Event expect, Event update) {
            return NEXT.weakCompareAndSet(this, expect, update);
        }

        Event tryLockPrev() {
            Event prev = this.prev;
            return (prev != null && prev != BUSY && PREV.weakCompareAndSet(this, prev, BUSY)) ? prev : null;
        }

        @Override
        public boolean remove() {
            while (true) {
                Event prev = this.prev;
                Event next = this.next;
                if (prev == null || next == null) {
                    return false;
                }
                if (prev == BUSY) {
                    Thread.yield();
                    continue;
                }
                if (!PREV.weakCompareAndSet(this, prev, BUSY)) {
                    Thread.yield();
                    continue;
                }
                if (next == BUSY) {
                    this.prev = prev;
                    Thread.yield();
                    continue;
                }
                Event nextPrev = null;
                if (next != TAIL) {
                    nextPrev = next.tryLockPrev();
                    if (nextPrev == null) {
                        this.prev = prev;
                        Thread.yield();
                        continue;
                    }
                    if (nextPrev != this) {
                        next.prev = nextPrev;
                        this.prev = prev;
                        Thread.yield();
                        continue;
                    }
                }
                if (prev.weakCasNext(this, next)) {
                    if (next != TAIL) {
                        next.prev = prev;
                    }
                    this.next = this.prev = null;
                    return true;
                } else {
                    if (next != TAIL) {
                        next.prev = nextPrev;
                    }
                    this.prev = prev;
                }
            }
        }

        public void run() {
            try {
                callback.run();
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    static class InvalidHandle implements EventHandle {

        @Override
        public boolean remove() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * Used for CAS.
     * For example:
     * <pre>
     *     static class DisposableImpl implements JDisposable {
     *
     *         // Create a handle placeholder representing invalid handle.
     *         private static final EventHandle INVALID = Schedule.createInvalidHandle();
     *         private volatile EventHandle handle;
     *         private static final AtomicReferenceFieldUpdater<DisposableImpl, EventHandle> HANDLE
     *                 = AtomicReferenceFieldUpdater.newUpdater(DisposableImpl.class, EventHandle.class, "handle");
     *
     *         void updateHandle(EventHandle newHandle) {
     *             while (true) {
     *                 EventHandle handle = this.handle;
     *                 if (handle == INVALID || HANDLE.weakCompareAndSet(this, handle, newHandle)) {
     *                     return;
     *                 }
     *             }
     *         }
     *
     *         public void dispose() {
     *             while (true) {
     *                 EventHandle handle = this.handle;
     *                 if (handle == INVALID) {
     *                     return;
     *                 }
     *                 if (HANDLE.weakCompareAndSet(this, handle, INVALID)) {
     *                     handle.remove();
     *                     return;
     *                 }
     *             }
     *         }
     *
     *         public boolean isDisposed() {
     *             return handle == INVALID;
     *         }
     *     }
     * </pre>
     * @return a invalid handle.
     *
     */
    public static EventHandle createInvalidHandle() {
        return new InvalidHandle();
    }

    /**
     * schedule the callback
     * @param time the time when the callback invoked.
     * @param unit the time unit.
     * @param callback the callback.
     */
    public EventHandle addEvent(long time, TimeUnit unit, Runnable callback) {
        return addEvent(unit.toNanos(time), callback);
    }

    /**
     * schedule the callback
     * @param nanoTime the time in nanoseconds when the callback invoked.
     * @param callback the callback.
     */
    public EventHandle addEvent(long nanoTime, Runnable callback) {
        long deadline = System.nanoTime() + nanoTime;
        while (true) {
            Event first = this.head.next;
            if (first == TAIL) {
                Event event = new Event(deadline, callback, TAIL, head);
                if (head.weakCasNext(TAIL, event)) {
                    signal();
                    return event;
                }
            } else if (first == BUSY) {
                Thread.yield();
            } else {
                if (deadline < first.deadline) {
                    Event prev = first.tryLockPrev();
                    if (prev == null) {
                        continue;
                    }
                    if (prev != head) {
                        first.prev = prev;
                        continue;
                    }
                    Event event = new Event(deadline, callback, first, head);
                    if (head.weakCasNext(first, event)) {
                        first.prev = event;
                        signal();
                        return event;
                    }
                } else {
                    Event node = first;
                    Event next = first.next;
                    while (next != null && next != TAIL && deadline >= next.deadline) {
                        node = next;
                        next = next.next;
                    }
                    if (next == BUSY) {
                        throw new RuntimeException("This is impossible");
                    }
                    if (next == null) {
                        Thread.yield();
                        continue;
                    }
                    if (next == TAIL) {
                        Event event = new Event(deadline, callback, TAIL, node);
                        if (node.weakCasNext(TAIL, event)) {
                            return event;
                        }
                    } else {
                        Event nextPrev = next.tryLockPrev();
                        if (nextPrev == null) {
                            Thread.yield();
                            continue;
                        }
                        Event event = new Event(deadline, callback, next, node);
                        if (node.weakCasNext(next, event)) {
                            next.prev = event;
                            return event;
                        } else {
                            next.prev = nextPrev;
                        }
                    }
                }
            }
        }
    }
}
