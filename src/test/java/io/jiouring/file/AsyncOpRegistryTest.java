package io.jiouring.file;

import io.netty.channel.IoEventLoop;
import io.netty.channel.MultiThreadIoEventLoopGroup;
import io.netty.channel.uring.IoUringIoHandler;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class AsyncOpRegistryTest {

    private static MultiThreadIoEventLoopGroup group;
    private static IoEventLoop eventLoop;

    @BeforeAll
    static void beforeAll() {
        group = new MultiThreadIoEventLoopGroup(1, IoUringIoHandler.newFactory());
        eventLoop = group.next();
    }

    @AfterAll
    static void afterAll() throws InterruptedException {
        group.shutdownGracefully().sync();
    }

    @Test
    void constructorWithValidSize() {
        AsyncOpRegistry registry = new AsyncOpRegistry(eventLoop, 100);
        assertTrue(registry.isEmpty());
    }

    @Test
    void constructorWithMaxSize() {
        AsyncOpRegistry registry = new AsyncOpRegistry(eventLoop, 65536);
        assertTrue(registry.isEmpty());
    }

    @Test
    void constructorWithSizeZero() {
        AsyncOpRegistry registry = new AsyncOpRegistry(eventLoop, 0);
        assertDoesNotThrow(() -> registry.acquire((byte) 1));
        assertTrue(registry.canAcquire(NativeConstants.IoRingOp.NOP));
    }

    @Test
    void isEmptyInitially() {
        AsyncOpRegistry registry = new AsyncOpRegistry(eventLoop, 100);
        assertTrue(registry.isEmpty());
    }

    @Test
    void notEmptyAfterNext() {
        AsyncOpRegistry registry = new AsyncOpRegistry(eventLoop, 100);
        registry.acquire((byte) 1);
        assertFalse(registry.isEmpty());
    }

    @Test
    void canAcquireWhenExhausted() {
        AsyncOpRegistry registry = new AsyncOpRegistry(eventLoop);
        for (int i = 0; i <= 0xFFFF; i++) registry.acquire(NativeConstants.IoRingOp.NOP);
        assertFalse(registry.canAcquire(NativeConstants.IoRingOp.NOP));
    }

    @Test
    void nextReturnsContext() {
        AsyncOpRegistry registry = new AsyncOpRegistry(eventLoop, 100);
        AsyncOpContext ctx = registry.acquire((byte) 22);
        assertNotNull(ctx);
        assertEquals(22, ctx.op);
        assertTrue(ctx.inUse);
    }

    @Test
    void nextWhenFullThrows() {
        AsyncOpRegistry registry = new AsyncOpRegistry(eventLoop);
        for (int i = 0; i <= 0xFFFF; i++) registry.acquire(NativeConstants.IoRingOp.NOP);
        assertThrows(IllegalStateException.class, () -> registry.acquire(NativeConstants.IoRingOp.NOP));
    }

    @Test
    void nextReturnsUniqueContexts() {
        AsyncOpRegistry registry = new AsyncOpRegistry(eventLoop, 100);
        Set<Short> ids = new HashSet<>();
        for (int i = 0; i < 50; i++) {
            AsyncOpContext ctx = registry.acquire((byte) 1);
            assertTrue(ids.add(ctx.id));
        }
    }

    @Test
    void nextSetsStartTime() {
        AsyncOpRegistry registry = new AsyncOpRegistry(eventLoop, 100);
        long before = System.nanoTime();
        AsyncOpContext ctx = registry.acquire((byte) 1);
        long after = System.nanoTime();
        assertTrue(ctx.startTime >= before);
        assertTrue(ctx.startTime <= after);
    }

    @Test
    void releaseFailsFuture() {
        AsyncOpRegistry registry = new AsyncOpRegistry(eventLoop, 100);
        AsyncOpContext ctx = registry.acquire((byte) 1);
        RuntimeException cause = new RuntimeException("test");
        registry.release(ctx, cause);
        assertThrows(RuntimeException.class, ctx.future::await);
    }

    @Test
    void releaseSetsInUseFalse() {
        AsyncOpRegistry registry = new AsyncOpRegistry(eventLoop, 100);
        AsyncOpContext ctx = registry.acquire((byte) 1);
        assertTrue(ctx.inUse);

        registry.release(ctx, new RuntimeException());
        assertFalse(ctx.inUse);
    }

    @Test
    void findStuckOpsReturnsEmptyWhenNoOps() {
        AsyncOpRegistry registry = new AsyncOpRegistry(eventLoop, 100);
        List<AsyncOpContext> stuck = registry.findStuckOps(1_000_000_000L);
        assertTrue(stuck.isEmpty());
    }

    @Test
    void findStuckOpsReturnsEmptyWhenNotStuck() {
        AsyncOpRegistry registry = new AsyncOpRegistry(eventLoop, 100);
        registry.acquire((byte) 1);

        List<AsyncOpContext> stuck = registry.findStuckOps(1_000_000_000_000L);
        assertTrue(stuck.isEmpty());
    }

    @Test
    void findStuckOpsFindsOldOps() throws Exception {
        AsyncOpRegistry registry = new AsyncOpRegistry(eventLoop, 100);
        AsyncOpContext ctx = registry.acquire((byte) 1);

        Thread.sleep(10);

        List<AsyncOpContext> stuck = registry.findStuckOps(1_000_000L);
        assertEquals(1, stuck.size());
        assertSame(ctx, stuck.get(0));
    }

    @Test
    void findStuckOpsOnlyReturnsInUse() throws Exception {
        AsyncOpRegistry registry = new AsyncOpRegistry(eventLoop, 100);
        AsyncOpContext ctx = registry.acquire((byte) 1);

        Thread.sleep(10);
        ctx.inUse = false;

        List<AsyncOpContext> stuck = registry.findStuckOps(1_000_000L);
        assertTrue(stuck.isEmpty());
    }

    @Test
    void findStuckOpsReturnsMultiple() throws Exception {
        AsyncOpRegistry registry = new AsyncOpRegistry(eventLoop, 100);
        AsyncOpContext ctx1 = registry.acquire((byte) 1);
        AsyncOpContext ctx2 = registry.acquire((byte) 2);

        Thread.sleep(10);

        List<AsyncOpContext> stuck = registry.findStuckOps(1_000_000L);
        assertEquals(2, stuck.size());
        assertTrue(stuck.contains(ctx1));
        assertTrue(stuck.contains(ctx2));
    }
}
