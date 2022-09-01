package party.iroiro.r2jdbc.util;

import lombok.extern.slf4j.Slf4j;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.locks.LockSupport;

/**
 * Dispatches {@link QueueItem}s to their {@link QueueItem#consumer}
 *
 * <p>The work is off-loaded to a scheduler of {@link Schedulers#parallel}.</p>
 *
 * @param <T> see {@link QueueItem}
 */
@Slf4j
public class QueueDispatcher<T> implements Runnable {
    private final SemiBlockingQueue<QueueItem<T>> queue;
    private final Scheduler scheduler;

    public QueueDispatcher() {
        this.queue = new SemiBlockingQueue<>();

        scheduler = Schedulers.parallel();
    }

    public SemiBlockingQueue<QueueItem<T>> subQueue() {
        return queue;
    }

    private void takeAndProcess() {
        try {
            scheduler.schedule(queue.take());
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void run() {
        queue.setConsumer(Thread.currentThread());
        log.debug("Listening");
        Thread.currentThread().setName("R2jdbcDispatcher");
        while (!Thread.interrupted()) {
            takeAndProcess();
        }
        log.debug("Cleaning up");
        while (queue.peek() != null) {
            takeAndProcess();
        }
        log.debug("Exiting");
    }

    public static void interrupt(Thread thread) {
        thread.interrupt();
        LockSupport.unpark(thread);
    }
}
