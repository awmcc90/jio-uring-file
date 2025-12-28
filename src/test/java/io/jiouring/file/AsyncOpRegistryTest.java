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
    void releaseFailsFuture() {
        AsyncOpRegistry registry = new AsyncOpRegistry(eventLoop, 100);
        AsyncOpContext ctx = registry.acquire((byte) 1);
        RuntimeException cause = new RuntimeException("test");
        registry.release(ctx, cause);
        assertThrows(RuntimeException.class, ctx::sync);
    }

    @Test
    void progressReturnsEmptyWhenNoOps() {
        AsyncOpRegistry registry = new AsyncOpRegistry(eventLoop, 100);
        List<AsyncOpContext> stuck = registry.progress(100);
        assertTrue(stuck.isEmpty());
    }

    @Test
    void progressReturnsEmptyWhenNoneStuck() {
        AsyncOpRegistry registry = new AsyncOpRegistry(eventLoop, 100);
        registry.acquire((byte) 1);

        List<AsyncOpContext> stuck = registry.progress(5);
        assertTrue(stuck.isEmpty());
    }

    @Test
    void progressFindsOldOps() {
        AsyncOpRegistry registry = new AsyncOpRegistry(eventLoop, 100);
        AsyncOpContext ctx = registry.acquire((byte) 1);
        List<AsyncOpContext> stuck = registry.progress(100);
        assertEquals(1, stuck.size());
        assertSame(ctx, stuck.get(0));
    }

    @Test
    void progressReturnsMultipleStuckOps() throws Exception {
        AsyncOpRegistry registry = new AsyncOpRegistry(eventLoop, 100);
        // These are gen 0
        AsyncOpContext ctx1 = registry.acquire((byte) 1);
        AsyncOpContext ctx2 = registry.acquire((byte) 2);

        List<AsyncOpContext> stuck = registry.progress(100);
        assertEquals(2, stuck.size());
        assertTrue(stuck.contains(ctx1));
        assertTrue(stuck.contains(ctx2));
    }
}
