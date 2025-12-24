package io.jiouring.file;

import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class AsyncOpContextTest {

    @Test
    void constructorSetsId() {
        AsyncOpContext ctx = new AsyncOpContext((short) 42);
        assertEquals(42, ctx.id);
    }

    @Test
    void constructorWithNegativeId() {
        AsyncOpContext ctx = new AsyncOpContext((short) -100);
        assertEquals(-100, ctx.id);
    }

    @Test
    void constructorWithMinValue() {
        AsyncOpContext ctx = new AsyncOpContext(Short.MIN_VALUE);
        assertEquals(Short.MIN_VALUE, ctx.id);
    }

    @Test
    void constructorWithMaxValue() {
        AsyncOpContext ctx = new AsyncOpContext(Short.MAX_VALUE);
        assertEquals(Short.MAX_VALUE, ctx.id);
    }

    @Test
    void initialStateNotInUse() {
        AsyncOpContext ctx = new AsyncOpContext((short) 1);
        assertFalse(ctx.inUse);
    }

    @Test
    void initialUringIdIsMinusOne() {
        AsyncOpContext ctx = new AsyncOpContext((short) 1);
        assertEquals(-1, ctx.uringId);
    }

    @Test
    void initialOpIsZero() {
        AsyncOpContext ctx = new AsyncOpContext((short) 1);
        assertEquals(0, ctx.op);
    }

    @Test
    void initialStartTimeIsZero() {
        AsyncOpContext ctx = new AsyncOpContext((short) 1);
        assertEquals(0, ctx.startTime);
    }

    @Test
    void resetSetsOp() {
        AsyncOpContext ctx = new AsyncOpContext((short) 1);
        ctx.reset((byte) 22);
        assertEquals(22, ctx.op);
    }

    @Test
    void resetSetsInUse() {
        AsyncOpContext ctx = new AsyncOpContext((short) 1);
        ctx.reset((byte) 1);
        assertTrue(ctx.inUse);
    }

    @Test
    void resetSetsStartTime() {
        AsyncOpContext ctx = new AsyncOpContext((short) 1);
        long before = System.nanoTime();
        ctx.reset((byte) 1);
        long after = System.nanoTime();
        assertTrue(ctx.startTime >= before);
        assertTrue(ctx.startTime <= after);
    }

    @Test
    void resetResetsUringId() {
        AsyncOpContext ctx = new AsyncOpContext((short) 1);
        ctx.uringId = 12345L;
        ctx.reset((byte) 1);
        assertEquals(-1, ctx.uringId);
    }

    @Test
    void resetResetsFuture() throws IOException {
        AsyncOpContext ctx = new AsyncOpContext((short) 1);
        ctx.reset((byte) 1);
        ctx.future.complete(42);
        assertEquals(42, ctx.future.join());

        ctx.reset((byte) 2);
        ctx.future.complete(100);
        assertEquals(100, ctx.future.join());
    }

    @Test
    void idDoesNotChangeOnReset() {
        AsyncOpContext ctx = new AsyncOpContext((short) 42);
        ctx.reset((byte) 1);
        assertEquals(42, ctx.id);
        ctx.reset((byte) 2);
        assertEquals(42, ctx.id);
    }

    @Test
    void multipleResetsWork() {
        AsyncOpContext ctx = new AsyncOpContext((short) 1);
        for (byte i = 0; i < 10; i++) {
            ctx.reset(i);
            assertEquals(i, ctx.op);
            assertTrue(ctx.inUse);
        }
    }

    @Test
    void resetAfterSettingUringId() {
        AsyncOpContext ctx = new AsyncOpContext((short) 1);
        ctx.reset((byte) 1);
        ctx.uringId = 999L;
        ctx.inUse = false;

        ctx.reset((byte) 2);
        assertEquals(-1, ctx.uringId);
        assertTrue(ctx.inUse);
    }

    @Test
    void futureIsNeverNull() {
        AsyncOpContext ctx = new AsyncOpContext((short) 1);
        assertNotNull(ctx.future);
        ctx.reset((byte) 1);
        assertNotNull(ctx.future);
    }

    @Test
    void sameFutureInstanceAfterReset() {
        AsyncOpContext ctx = new AsyncOpContext((short) 1);
        SyscallFuture f1 = ctx.future;
        ctx.reset((byte) 1);
        SyscallFuture f2 = ctx.future;
        assertSame(f1, f2);
    }

    @Test
    void uringIdCanBeModified() {
        AsyncOpContext ctx = new AsyncOpContext((short) 1);
        ctx.reset((byte) 1);
        ctx.uringId = Long.MAX_VALUE;
        assertEquals(Long.MAX_VALUE, ctx.uringId);
    }

    @Test
    void inUseCanBeCleared() {
        AsyncOpContext ctx = new AsyncOpContext((short) 1);
        ctx.reset((byte) 1);
        assertTrue(ctx.inUse);
        ctx.inUse = false;
        assertFalse(ctx.inUse);
    }

    @Test
    void opWithMaxByteValue() {
        AsyncOpContext ctx = new AsyncOpContext((short) 1);
        ctx.reset(Byte.MAX_VALUE);
        assertEquals(Byte.MAX_VALUE, ctx.op);
    }

    @Test
    void opWithMinByteValue() {
        AsyncOpContext ctx = new AsyncOpContext((short) 1);
        ctx.reset(Byte.MIN_VALUE);
        assertEquals(Byte.MIN_VALUE, ctx.op);
    }

    @Test
    void startTimeIncrementsOnReset() throws Exception {
        AsyncOpContext ctx = new AsyncOpContext((short) 1);
        ctx.reset((byte) 1);
        long first = ctx.startTime;

        Thread.sleep(1);

        ctx.reset((byte) 2);
        long second = ctx.startTime;

        assertTrue(second > first);
    }
}
