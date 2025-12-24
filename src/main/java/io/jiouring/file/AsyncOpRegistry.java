package io.jiouring.file;

import io.netty.channel.uring.IoUringIoEvent;
import io.netty.util.internal.SystemPropertyUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class AsyncOpRegistry implements Iterable<AsyncOpContext> {

    private static final Logger logger = LogManager.getLogger(AsyncOpRegistry.class);

    private final int maxInFlight;
    private final AsyncOpContext[] contextPool;
    private final int[] freeIndices;
    private int freeTop;

    public AsyncOpRegistry() {
        this(SystemPropertyUtil.getInt("io.netty.iouring.ringSize", 4096) * 2);
    }

    public AsyncOpRegistry(int maxInFlight) {
        if (maxInFlight > 65536) {
            throw new IllegalArgumentException("Cannot exceed 64k slots due to Short ID mapping");
        }

        this.maxInFlight = maxInFlight;

        this.freeIndices = new int[maxInFlight];
        for (int i = 0; i < maxInFlight; i++) this.freeIndices[i] = i;

        // -1 for 0 based index adjustment
        // -1 for special id field reservation
        this.freeTop = maxInFlight - 2;

        this.contextPool = new AsyncOpContext[maxInFlight];
        for (int i = 0; i < maxInFlight; i++) {
            short id = (short) (Short.MIN_VALUE + i);
            this.contextPool[i] = new AsyncOpContext(id);
        }
    }

    public boolean isEmpty() {
        return freeTop == maxInFlight - 2;
    }

    public boolean isFull() {
        return freeTop < 0;
    }

    public AsyncOpContext next(byte op) {
        if (freeTop < 0) {
            throw new IllegalStateException("Registry full; too many in-flight ops");
        }

        int index = freeIndices[freeTop--];
        AsyncOpContext ctx = contextPool[index];
        ctx.reset(op);

        return ctx;
    }

    public void complete(IoUringIoEvent event) {
        short id = event.data();

        int index = id - Short.MIN_VALUE;

        if (index < maxInFlight) {
            AsyncOpContext ctx = contextPool[index];

            if (ctx.inUse) {
                ctx.future.complete(event.res());
                ctx.inUse = false;
                ctx.uringId = -1;

                // Push back to free stack
                freeIndices[++freeTop] = index;
            }
        }
    }

    // Only release if it was actually in use to prevent double-free corruption
    public void release(AsyncOpContext ctx, Throwable cause) {
        if (ctx.inUse) {
            ctx.inUse = false;
            ctx.uringId = -1;
            ctx.future.fail(cause);

            int index = ctx.id - Short.MIN_VALUE;
            freeIndices[++freeTop] = index;
        }
    }

    public List<AsyncOpContext> findStuckOps(long timeoutNs) {
        long now = System.nanoTime();
        List<AsyncOpContext> stuck = null;

        for (int i = 0; i < maxInFlight; i++) {
            AsyncOpContext ctx = contextPool[i];
            if (ctx.inUse && (now - ctx.startTime) > timeoutNs) {
                if (stuck == null) {
                    stuck = new ArrayList<>(4);
                }
                stuck.add(ctx);
            }
        }
        return stuck != null ? stuck : Collections.emptyList();
    }

    @Override
    public Iterator<AsyncOpContext> iterator() {
        return new Iterator<>() {
            private int currentIdx = 0;
            private AsyncOpContext nextElement = null;

            {
                findNext();
            }

            private void findNext() {
                nextElement = null;
                while (currentIdx < maxInFlight) {
                    AsyncOpContext ctx = contextPool[currentIdx++];
                    if (ctx.inUse) {
                        nextElement = ctx;
                        break;
                    }
                }
            }

            @Override
            public boolean hasNext() {
                return nextElement != null;
            }

            @Override
            public AsyncOpContext next() {
                if (nextElement == null) {
                    throw new NoSuchElementException();
                }
                AsyncOpContext res = nextElement;
                findNext();
                return res;
            }
        };
    }
}