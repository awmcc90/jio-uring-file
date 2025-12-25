package io.jiouring.file;

import io.netty.channel.uring.IoUringIoEvent;
import io.netty.util.internal.SystemPropertyUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

class AsyncOpRegistry  {

    private static final Logger logger = LoggerFactory.getLogger(AsyncOpRegistry.class);

    private final AsyncOpArena arena;

    AsyncOpRegistry() {
        this(SystemPropertyUtil.getInt("io.netty.iouring.ringSize", 4096) * 2);
    }

    AsyncOpRegistry(int maxInFlight) {
        arena = new AsyncOpArena(maxInFlight);
    }

    boolean isEmpty() {
        return arena.isEmpty();
    }

    boolean isFull() {
        return arena.isFull();
    }

    AsyncOpContext acquire(byte op) {
        return arena.acquire(op);
    }

    void complete(IoUringIoEvent event) {
        short id = event.data();
        AsyncOpContext ctx = arena.get(id);

        if (ctx == null) {
            logger.error("Completion for event {} failed because it was missing from arena", event);
            return;
        }

        if (ctx.inUse) {
            ctx.future.complete(event.res());
            arena.release(ctx);
        }
    }

    // Only release if it was actually in use to prevent double-free corruption
    void release(AsyncOpContext ctx, Throwable cause) {
        if (ctx.inUse) {
            ctx.future.fail(cause);
            arena.release(ctx);
        }
    }

    List<AsyncOpContext> findStuckOps(long timeoutNs) {
        long now = System.nanoTime();
        List<AsyncOpContext> stuck = null;

        for (int i = 0; i < arena.capacity(); i++) {
            AsyncOpContext ctx = arena.get(i);
            if (ctx != null && ctx.inUse && (now - ctx.startTime) > timeoutNs) {
                if (stuck == null) stuck = new ArrayList<>(4);
                stuck.add(ctx);
            }
        }
        return stuck != null ? stuck : Collections.emptyList();
    }
}