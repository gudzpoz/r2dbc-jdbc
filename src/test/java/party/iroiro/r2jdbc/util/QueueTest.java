package party.iroiro.r2jdbc.util;

import lbmq.LinkedBlockingMultiQueue;
import org.junit.jupiter.api.Test;
import party.iroiro.r2jdbc.JdbcPacket;

import java.util.function.BiConsumer;

public class QueueTest {
    @Test
    public void multiThreadedQueue() {
        System.out.println("Main " + Thread.currentThread().getName());
        QueueDispatcher<JdbcPacket> adapter = new QueueDispatcher<>(new LinkedBlockingMultiQueue<>());
        Thread dispatcher = new Thread(adapter);
        dispatcher.start();
        LinkedBlockingMultiQueue<Integer, QueueItem<JdbcPacket>>.SubQueue sub = adapter.subQueue();
        BiConsumer<JdbcPacket, Throwable> consumer =
                (p, e) -> System.out.println(Thread.currentThread().getName() + " " + e.getMessage());
        new Thread(() -> {
            System.out.println("From " + Thread.currentThread().getName());
            for (int i = 0; i < 20; i++) {
                sub.offer(new QueueItem<>(null, new Exception(String.valueOf(i)), consumer, true));
            }
        }).start();
        for (int i = 20; i < 40; i++) {
            sub.offer(new QueueItem<>(null, new Exception(String.valueOf(i)), consumer, false));
        }
        dispatcher.interrupt();
        while (dispatcher.isAlive()) {
            try {
                dispatcher.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void interruptToQuit() {
        QueueDispatcher<Integer> dispatcher = new QueueDispatcher<>(
                new LinkedBlockingMultiQueue<>()
        );
        Thread thread = new Thread(dispatcher);
        thread.start();
        try {
            Thread.sleep(1);
        } catch (InterruptedException ignored) {
        }
        thread.interrupt();
        try {
            thread.join();
        } catch (InterruptedException ignored) {
        }
    }
}
