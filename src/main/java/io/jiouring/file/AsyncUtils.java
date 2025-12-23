package io.jiouring.file;

import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

import java.util.concurrent.CompletableFuture;

public class AsyncUtils {

    private AsyncUtils() {}

    public static <T> void completeFrom(CompletableFuture<T> target, CompletableFuture<? extends T> source) {
        source.whenComplete((v, t) -> {
            if (t != null) {
                target.completeExceptionally(t);
            } else {
                target.complete(v);
            }
        });
    }

    public static <T> CompletableFuture<T> toCompletableFuture(Future<T> nettyFuture) {
        CompletableFuture<T> cf = new CompletableFuture<>();
        completeFrom(cf, nettyFuture);
        return cf;
    }

    public static <T> void completeFrom(CompletableFuture<T> target, Future<? extends T> nettyFuture) {
        nettyFuture.addListener(f -> {
            if (f.isSuccess()) {
                @SuppressWarnings("unchecked")
                T result = (T) f.getNow();
                target.complete(result);
            } else {
                target.completeExceptionally(f.cause());
            }
        });
    }

    public static <T> void completeFrom(CompletableFuture<T> target, SyscallFuture syscallFuture) {
        syscallFuture.onComplete((res, err) -> {
            if (err != null) target.completeExceptionally(err);
            else target.complete(null);
        });
    }

    public static <T> void completeInto(CompletableFuture<T> source, Promise<T> target) {
        source.whenComplete((v, t) -> {
            if (t != null) {
                target.setFailure(t);
            } else {
                target.setSuccess(v);
            }
        });
    }
}