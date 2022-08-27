package party.iroiro.r2jdbc.util;

import org.junit.jupiter.api.Test;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.*;

class SemiBlockingQueueTest {
    @Test
    public void illegalTest() {
        SemiBlockingQueue<Integer> queue = new SemiBlockingQueue<>();
        queue.setConsumer(new Thread(() -> {}));
        assertThrows(UnsupportedOperationException.class, () -> queue.setConsumer(Thread.currentThread()));
        assertDoesNotThrow(() -> assertNull(queue.take()));
    }

    @Test
    public void interruptionTest() throws InterruptedException {
        SemiBlockingQueue<Integer> queue = new SemiBlockingQueue<>();
        AtomicReference<Throwable> throwable = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            queue.setConsumer(Thread.currentThread());
            try {
                assertEquals(1, queue.take());
                queue.take();
                fail();
            } catch (Throwable e) {
                throwable.set(e);
            }
        });
        thread.start();
        Thread.sleep(200);
        assertTrue(thread.isAlive());
        queue.offer(1);
        assertTrue(thread.isAlive());

        LockSupport.unpark(thread);
        Thread.sleep(200);
        assertTrue(thread.isAlive());

        thread.interrupt();
        LockSupport.unpark(thread);
        Thread.sleep(200);
        assertFalse(thread.isAlive());

        assertInstanceOf(InterruptedException.class, throwable.get());
    }

    @Test
    public void simpleQueueTest() throws InterruptedException {
        SemiBlockingQueue<Integer> queue = new SemiBlockingQueue<>();
        queue.offer(1);
        queue.offer(2);
        AtomicReference<Throwable> throwable = new AtomicReference<>(null);
        Thread thread = new Thread(() -> {
            queue.setConsumer(Thread.currentThread());
            try {
                assertEquals(1, queue.take());
                assertDoesNotThrow(queue::element);
                assertEquals(2, queue.take());
                assertNull(queue.peek());
                assertThrows(NoSuchElementException.class, queue::element);
                for (int i = 3; i < 10; i++) {
                    assertEquals(i, queue.take());
                }
            } catch (Throwable e) {
                throwable.set(e);
            }
        });
        thread.start();
        for (int i = 3; i < 10; i++) {
            Thread.sleep(500);
            queue.offer(i);
        }
        Thread.sleep(200);
        assertFalse(thread.isAlive());
        if (throwable.get() != null) {
            fail(throwable.get());
        }
    }

    @Test
    public void concurrentTest() throws InterruptedException {
        SemiBlockingQueue<Integer> queue = new SemiBlockingQueue<>();
        AtomicReference<Throwable> throwable = new AtomicReference<>(null);
        Thread consumer = new Thread(() -> {
            queue.setConsumer(Thread.currentThread());
            try {
                //noinspection StatementWithEmptyBody
                while (queue.take() < 1_000_000);
            } catch (Throwable e) {
                throwable.set(e);
            }
        });
        consumer.start();
        for (int i = 0; i < 10_000_000; i += 10_000) {
            int finalI = i;
            new Thread(() -> {
                for (int j = 1; j <= 10_000; j++) {
                    queue.offer(finalI + j);
                }
            }).start();
        }
        consumer.join();
        if (throwable.get() != null) {
            fail(throwable.get());
        }
    }
}