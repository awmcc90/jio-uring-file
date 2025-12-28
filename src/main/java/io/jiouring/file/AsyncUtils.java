package io.jiouring.file;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

import java.util.concurrent.CompletableFuture;

public final class AsyncUtils {

    private AsyncUtils() {}

    public static <T> void completeFrom(CompletableFuture<T> target, CompletableFuture<? super T> source) {
        source.whenComplete((v, err) -> {
            if (err != null) target.completeExceptionally(err);
            else target.complete((T) v);
        });
    }

    public static <T> void completeFrom(CompletableFuture<T> target, Future<? super T> source) {
        source.addListener((f) -> {
            if (!f.isSuccess()) target.completeExceptionally(f.cause());
            else target.complete((T) f.getNow());
        });
    }

    public static <T> void completeFrom(Promise<T> target, Future<? super T> source) {
        source.addListener((f) -> {
            if (!f.isSuccess()) target.tryFailure(f.cause());
            else target.trySuccess((T) f.getNow());
        });
    }
}