package io.jiouring.file;

final class AsyncOpArena {

    private final int capacity;
    private final AsyncOpContext[] pool;
    private final int[] free;
    private int top;

    AsyncOpArena(int capacity) {
        if (capacity > 65536) {
            throw new IllegalArgumentException("Capacity cannot exceed 65536");
        }

        this.capacity = capacity;

        this.free = new int[capacity];
        for (int i = 0; i < capacity; i++) free[i] = i;
        this.top = capacity - 1;

        this.pool = new AsyncOpContext[capacity];
        for (int i = 0; i < capacity; i++) {
            short id = (short) (Short.MIN_VALUE + i);
            pool[i] = new AsyncOpContext(id);
        }
    }

    int capacity() {
        return capacity;
    }

    boolean isEmpty() {
        return top == capacity - 1;
    }

    boolean isFull() {
        return top < 0;
    }

    AsyncOpContext acquire(byte op) {
        if (top < 0) {
            throw new IllegalStateException("AsyncOpArena is full");
        }

        int idx = free[top--];
        AsyncOpContext ctx = pool[idx];
        ctx.reset(op);
        return ctx;
    }

    void release(AsyncOpContext ctx) {
        int idx = ctx.id - Short.MIN_VALUE;
        if (idx < capacity && pool[idx] == ctx) {
            if (ctx.inUse) {
                ctx.inUse = false;
                ctx.uringId = -1;
                free[++top] = idx;
            }
        }
    }

    AsyncOpContext get(int id) {
        return id >= 0 && id < capacity ? pool[id] : null;
    }

    AsyncOpContext get(short id) {
        int idx = id - Short.MIN_VALUE;
        return get(idx);
    }
}
