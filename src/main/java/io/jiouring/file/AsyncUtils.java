package io.jiouring.file;

import java.util.concurrent.CompletableFuture;

public final class AsyncUtils {

    private AsyncUtils() {}

    public static <T> void completeFrom(CompletableFuture<T> target, CompletableFuture<? extends T> source) {
        source.whenComplete((v, err) -> {
            if (err != null) target.completeExceptionally(err);
            else target.complete(v);
        });
    }

    public static void completeFrom(CompletableFuture<Integer> target, SyscallFuture source) {
        source.onComplete((res, err) -> {
            if (err != null) target.completeExceptionally(err);
            else target.complete(res);
        });
    }

    public static void completeFrom(SyscallFuture target, SyscallFuture source) {
        source.onComplete((res, err) -> {
            if (err != null) target.fail(err);
            else target.complete(res);
        });
    }
}