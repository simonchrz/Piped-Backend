package me.kavin.piped.utils;

import java.util.concurrent.*;
import java.util.function.Supplier;

public class Multithreading {

    private static final ExecutorService es = Executors.newVirtualThreadPerTaskExecutor();
    // Cap at availableProcessors() (= 4 on Pi5) instead of *8 (= 32). The
    // pool is used by callers that fan-out YouTube fetches (saveChannel,
    // sub-import). 32-wide bursts trigger YT's anti-bot SignInConfirm
    // IP-flag — same failure mode as the PubSub patches in Main.java +
    // PubSubHandlers.java.
    private static final ExecutorService esLimited = Executors
            .newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private static final ExecutorService esLimitedPubSub = Executors
            .newFixedThreadPool(Runtime.getRuntime().availableProcessors());
    private static final ForkJoinPool forkJoinPool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());

    public static void runAsync(final Runnable runnable) {
        es.submit(runnable);
    }

    public static void runAsyncTask(final ForkJoinTask<?> task) {
        forkJoinPool.submit(task);
    }

    public static void runAsyncLimited(final Runnable runnable) {
        esLimited.submit(runnable);
    }

    public static void runAsyncLimitedPubSub(final Runnable runnable) {
        esLimitedPubSub.submit(runnable);
    }

    public static ExecutorService getCachedExecutor() {
        return es;
    }

    public static <U> Future<U> supplyAsync(Supplier<U> supplier) {
        return es.submit(supplier::get);
    }
}
