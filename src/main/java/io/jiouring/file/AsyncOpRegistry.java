package io.jiouring.file;

import io.netty.channel.uring.IoUringIoEvent;
import io.netty.util.concurrent.EventExecutor;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

class AsyncOpRegistry  {

    private static final Logger logger = LoggerFactory.getLogger(AsyncOpRegistry.class);
    private static final int MAX_GENERATION_GAP = 30;

    private final EventExecutor executor;
    private final OpIdPool opIdPool;
    private final Int2ObjectOpenHashMap<AsyncOpContext> contextLookup;
    private int generation = 0;

    AsyncOpRegistry(EventExecutor executor, int initialCapacity) {
        this.executor = executor;
        this.opIdPool = new OpIdPool();
        this.contextLookup = new Int2ObjectOpenHashMap<>(initialCapacity);
    }

    AsyncOpRegistry(EventExecutor executor) {
        this(executor, 4096);
    }

    boolean isEmpty() {
        return contextLookup.isEmpty();
    }

    boolean canAcquire(byte op) {
        return opIdPool.canAcquire(op);
    }

    AsyncOpContext acquire(byte op) {
        short id = opIdPool.acquire(op);
        AsyncOpContext ctx = new AsyncOpContext(executor, op, id, generation);
        contextLookup.put(key(op, id), ctx);
        return ctx;
    }

    void complete(IoUringIoEvent event) {
        int key = key(event.opcode(), event.data());
        AsyncOpContext ctx = contextLookup.remove(key);

        if (ctx == null) {
            logger.error("Completion for event {} failed because it was missing from arena", event);
            return;
        }

        ctx.trySuccess(event.res());
        opIdPool.release(ctx.op, ctx.id);
    }

    void release(AsyncOpContext ctx, Throwable cause) {
        ctx.tryFailure(cause);
        contextLookup.remove(key(ctx.op, ctx.id));
        opIdPool.release(ctx.op, ctx.id);
    }

    List<AsyncOpContext> progress(int generation) {
        if (generation <= this.generation) {
            throw new IllegalArgumentException("generation <= " + this.generation + " (was " + generation + ")");
        }

        this.generation = generation;

        List<AsyncOpContext> stuck = null;
        for (AsyncOpContext ctx : contextLookup.values()) {
            if (ctx != null && (generation - ctx.generation) > MAX_GENERATION_GAP) {
                if (stuck == null) stuck = new ArrayList<>(4);
                stuck.add(ctx);
            }
        }
        return stuck != null ? stuck : Collections.emptyList();
    }

    private static int key(byte op, short id) {
        return ((op & 0xFF) << 16) | (id & 0xFFFF);
    }

    private static final class OpIdPool {
        private static final int SIZE = 1 << 16;

        private final short[] next = new short[56];
        private final BitSet[] inUse = new BitSet[56];
        private final int[] used = new int[56];

        private boolean canAcquire(byte op) {
            return used[op] < SIZE;
        }

        private short acquire(byte op) {
            if (used[op] == SIZE) {
                throw new IllegalStateException("op " + op + " exhausted");
            }

            BitSet bs = inUse[op];

            // Lazy load these because memory
            if (bs == null) {
                bs = inUse[op] = new BitSet(SIZE);
            }

            int start = next[op] & 0xFFFF;

            int idx = bs.nextClearBit(start);
            if (idx >= SIZE) {
                idx = bs.nextClearBit(0);
            }

            bs.set(idx);
            used[op]++;
            next[op] = (short) (idx + 1);
            return (short) idx;
        }

        private void release(byte op, short id) {
            int idx = id & 0xFFFF;
            if (inUse[op].get(idx)) {
                inUse[op].clear(idx);
                used[op]--;
            }
        }
    }
}