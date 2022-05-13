package party.iroiro.r2jdbc.util;

import lbmq.LinkedBlockingMultiQueue;
import lombok.extern.slf4j.Slf4j;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Dispatches {@link QueueItem}s to their {@link QueueItem#consumer}
 *
 * <p>The work may be off-loaded to a scheduler of {@link Schedulers#parallel}
 * depending on {@link QueueItem#parallel}. Or else it might slow down the dispatcher
 * thread.</p>
 *
 * @param <T> see {@link QueueItem}
 */
@Slf4j
public class QueueDispatcher<T> implements Runnable {
    private final LinkedBlockingMultiQueue<Integer, QueueItem<T>> queue;
    private final AtomicInteger i;
    private final Scheduler scheduler;

    public QueueDispatcher(LinkedBlockingMultiQueue<Integer, QueueItem<T>> queue) {
        this.queue = queue;
        this.i = new AtomicInteger(1);

        scheduler = Schedulers.parallel();
    }

    public LinkedBlockingMultiQueue<Integer, QueueItem<T>>.SubQueue subQueue() {
        int id = i.getAndAdd(1);
        queue.addSubQueue(id, 10);
        return queue.getSubQueue(id);
    }

    private void takeAndProcess() {
        try {
            QueueItem<T> item = queue.take();
            if (item.parallel) {
                scheduler.schedule(() -> item.consumer.accept(item.item, item.e));
            } else {
                item.consumer.accept(item.item, item.e);
            }
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void run() {
        log.trace("Listening");
        Thread.currentThread().setName("R2jdbcDispatcher");
        while (!Thread.interrupted()) {
            takeAndProcess();
        }
        log.trace("Cleaning up");
        while (queue.peek() != null) {
            takeAndProcess();
        }
        log.trace("Exiting");
    }
}
