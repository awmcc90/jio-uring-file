package io.jiouring.file;

import io.netty.channel.uring.IoUringIoEvent;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

class AsyncOpRegistry  {

    private static final Logger logger = LoggerFactory.getLogger(AsyncOpRegistry.class);

    private final OpIdPool opIdPool;
    private final Int2ObjectOpenHashMap<AsyncOpContext> contextLookup;

    AsyncOpRegistry(int initialCapacity) {
        opIdPool = new OpIdPool();
        contextLookup = new Int2ObjectOpenHashMap<>(initialCapacity);
    }

    AsyncOpRegistry() {
        this(4096);
    }

    boolean isEmpty() {
        return contextLookup.isEmpty();
    }

    boolean canAcquire(byte op) {
        return opIdPool.canAcquire(op);
    }

    AsyncOpContext acquire(byte op) {
        short id = opIdPool.acquire(op);
        AsyncOpContext ctx = new AsyncOpContext(op, id);
        contextLookup.put(key(op, id), ctx);
        return ctx;
    }

    void complete(IoUringIoEvent event) {
        byte op = event.opcode();
        short id = event.data();
        int key = key(op, id);
        AsyncOpContext ctx = contextLookup.get(key);

        if (ctx == null) {
            logger.error("Completion for event {} failed because it was missing from arena", event);
            return;
        }

        if (ctx.inUse) {
            ctx.future.complete(event.res());
            contextLookup.remove(key(ctx.op, ctx.id));
            opIdPool.release(ctx.op, ctx.id);
            ctx.inUse = false;
        }
    }

    void release(AsyncOpContext ctx, Throwable cause) {
        if (ctx.inUse) {
            ctx.future.fail(cause);
            contextLookup.remove(key(ctx.op, ctx.id));
            opIdPool.release(ctx.op, ctx.id);
            ctx.inUse = false;
        }
    }

    List<AsyncOpContext> findStuckOps(long timeoutNs) {
        long now = System.nanoTime();
        List<AsyncOpContext> stuck = null;

        for (AsyncOpContext ctx : contextLookup.values()) {
            if (ctx != null && ctx.inUse && (now - ctx.startTime) > timeoutNs) {
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
        private static final int MAX = 1 << 16;

        private final short[] next = new short[56];
        private final BitSet[] inUse = new BitSet[56];
        private final int[] used = new int[56];

        private OpIdPool() {
            for (int i = 0; i < inUse.length; i++) {
                inUse[i] = new BitSet(MAX);
            }
        }

        private boolean canAcquire(byte op) {
            return used[op] < MAX;
        }

        private short acquire(byte op) {
            if (used[op] == MAX) {
                throw new IllegalStateException("op " + op + " exhausted");
            }

            BitSet bs = inUse[op];
            int start = next[op] & 0xFFFF;

            int id = bs.nextClearBit(start);
            if (id >= MAX) {
                id = bs.nextClearBit(0);
            }

            bs.set(id);
            used[op]++;
            next[op] = (short) (id + 1);
            return (short) id;
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