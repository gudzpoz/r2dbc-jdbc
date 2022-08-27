package party.iroiro.r2jdbc.util;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

/**
 * A queue that allows multiple producers to offer non-blocking-ly to blocking consumers
 */
public class SemiBlockingQueue<T> {
    private final ConcurrentLinkedQueue<T> queue = new ConcurrentLinkedQueue<>();
    private final AtomicInteger size = new AtomicInteger(0);
    private final AtomicReference<Thread> consumer = new AtomicReference<>(null);

    public void setConsumer(Thread consumer) {
        if (!this.consumer.compareAndSet(null, consumer)) {
            throw new UnsupportedOperationException("Only one consumer supported");
        }
    }

    public void offer(T t) {
        queue.offer(t);
        tryNotify();
    }

    private void tryNotify() {
        if (size.getAndIncrement() < 0) {
            /*
             * Quoting the Javadoc:
             *     ... Otherwise, its next call to park is guaranteed not to block. ...
             * So we can be assured that it will not deadlock.
             */
            LockSupport.unpark(consumer.get());
        }
    }

    public T poll() {
        T t = queue.poll();
        if (t != null) {
            size.decrementAndGet();
        }
        return t;
    }

    public T element() {
        return queue.element();
    }

    public T peek() {
        return queue.peek();
    }

    public T take() throws InterruptedException {
        if (Thread.currentThread().equals(consumer.get())) {
            T t = poll();
            while (t == null) {
                if (Thread.currentThread().isInterrupted()) {
                    throw new InterruptedException();
                }
                if (size.compareAndSet(0, -1)) {
                    LockSupport.park();
                    size.incrementAndGet();
                }
                t = poll();
            }
            return t;
        } else {
            return null;
        }
    }
}
