package party.iroiro.r2jdbc.util;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

public class SingletonTest {
    @Test
    public void singletonTest() {
        SingletonMono<String> singletonMono = new SingletonMono<>();
        Flux.merge(
                singletonMono.get().delayElement(Duration.ofMillis(100)),
                Mono.just("Test").delayElement(Duration.ofMillis(200)).doOnNext(singletonMono::set),
                singletonMono.get().delayElement(Duration.ofMillis(300)),
                singletonMono.get().delayElement(Duration.ofMillis(400)),
                Mono.just("then").delayElement(Duration.ofSeconds(1))
                        .flatMap(t -> singletonMono.get()).delayElement(Duration.ofMillis(500))
        ).as(StepVerifier::create)
                .expectNext("Test", "Test", "Test", "Test", "Test")
                .verifyComplete();
    }
}
