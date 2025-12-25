package io.jiouring.file;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class AsyncOpRegistryTest {

    @Test
    void constructorWithValidSize() {
        AsyncOpRegistry registry = new AsyncOpRegistry(100);
        assertTrue(registry.isEmpty());
    }

    @Test
    void constructorWithMaxSize() {
        AsyncOpRegistry registry = new AsyncOpRegistry(65536);
        assertTrue(registry.isEmpty());
    }

    @Test
    void constructorExceedingMaxThrows() {
        assertThrows(IllegalArgumentException.class, () -> new AsyncOpRegistry(65537));
    }

    @Test
    void constructorWithZeroSize() {
        AsyncOpRegistry registry = new AsyncOpRegistry(0);
        assertTrue(registry.isFull());
    }

    @Test
    void constructorWithSizeOne() {
        AsyncOpRegistry registry = new AsyncOpRegistry(1);
        registry.next((byte) 1);
        assertTrue(registry.isFull());
    }

    @Test
    void constructorWithSizeTwo() {
        AsyncOpRegistry registry = new AsyncOpRegistry(2);
        assertTrue(registry.isEmpty());
        assertFalse(registry.isFull());
    }

    @Test
    void isEmptyInitially() {
        AsyncOpRegistry registry = new AsyncOpRegistry(100);
        assertTrue(registry.isEmpty());
    }

    @Test
    void notEmptyAfterNext() {
        AsyncOpRegistry registry = new AsyncOpRegistry(100);
        registry.next((byte) 1);
        assertFalse(registry.isEmpty());
    }

    @Test
    void isFullWhenExhausted() {
        AsyncOpRegistry registry = new AsyncOpRegistry(3);
        registry.next((byte) 1);
        registry.next((byte) 1);
        registry.next((byte) 1);
        assertTrue(registry.isFull());
    }

    @Test
    void nextReturnsContext() {
        AsyncOpRegistry registry = new AsyncOpRegistry(100);
        AsyncOpContext ctx = registry.next((byte) 22);
        assertNotNull(ctx);
        assertEquals(22, ctx.op);
        assertTrue(ctx.inUse);
    }

    @Test
    void nextWhenFullThrows() {
        AsyncOpRegistry registry = new AsyncOpRegistry(3);
        registry.next((byte) 1);
        registry.next((byte) 1);
        registry.next((byte) 1);
        assertThrows(IllegalStateException.class, () -> registry.next((byte) 1));
    }

    @Test
    void nextReturnsUniqueContexts() {
        AsyncOpRegistry registry = new AsyncOpRegistry(100);
        Set<Short> ids = new HashSet<>();
        for (int i = 0; i < 50; i++) {
            AsyncOpContext ctx = registry.next((byte) 1);
            assertTrue(ids.add(ctx.id));
        }
    }

    @Test
    void nextSetsStartTime() {
        AsyncOpRegistry registry = new AsyncOpRegistry(100);
        long before = System.nanoTime();
        AsyncOpContext ctx = registry.next((byte) 1);
        long after = System.nanoTime();
        assertTrue(ctx.startTime >= before);
        assertTrue(ctx.startTime <= after);
    }

    @Test
    void releaseReturnsContextToPool() {
        AsyncOpRegistry registry = new AsyncOpRegistry(3);
        AsyncOpContext ctx = registry.next((byte) 1);
        registry.next((byte) 1);
        registry.next((byte) 1);
        assertTrue(registry.isFull());

        registry.release(ctx, new RuntimeException());
        assertFalse(registry.isFull());
    }

    @Test
    void releaseFailsFuture() {
        AsyncOpRegistry registry = new AsyncOpRegistry(100);
        AsyncOpContext ctx = registry.next((byte) 1);
        RuntimeException cause = new RuntimeException("test");

        registry.release(ctx, cause);

        assertThrows(RuntimeException.class, () -> ctx.future.join());
    }

    @Test
    void releaseSetsInUseFalse() {
        AsyncOpRegistry registry = new AsyncOpRegistry(100);
        AsyncOpContext ctx = registry.next((byte) 1);
        assertTrue(ctx.inUse);

        registry.release(ctx, new RuntimeException());
        assertFalse(ctx.inUse);
    }

    @Test
    void releaseResetsUringId() {
        AsyncOpRegistry registry = new AsyncOpRegistry(100);
        AsyncOpContext ctx = registry.next((byte) 1);
        ctx.uringId = 999L;

        registry.release(ctx, new RuntimeException());
        assertEquals(-1, ctx.uringId);
    }

    @Test
    void releaseWhenNotInUseDoesNothing() {
        AsyncOpRegistry registry = new AsyncOpRegistry(3);
        AsyncOpContext ctx = registry.next((byte) 1);
        registry.next((byte) 1);
        registry.next((byte) 1);
        assertTrue(registry.isFull());

        ctx.inUse = false;
        registry.release(ctx, new RuntimeException());
        assertTrue(registry.isFull());
    }

    @Test
    void doubleReleaseIgnored() {
        AsyncOpRegistry registry = new AsyncOpRegistry(3);
        AsyncOpContext ctx = registry.next((byte) 1);
        registry.next((byte) 1);
        registry.next((byte) 1);
        assertTrue(registry.isFull());

        registry.release(ctx, new RuntimeException());
        assertFalse(registry.isFull());

        registry.release(ctx, new RuntimeException());
        assertFalse(registry.isFull());
    }

    @Test
    void findStuckOpsReturnsEmptyWhenNoOps() {
        AsyncOpRegistry registry = new AsyncOpRegistry(100);
        List<AsyncOpContext> stuck = registry.findStuckOps(1_000_000_000L);
        assertTrue(stuck.isEmpty());
    }

    @Test
    void findStuckOpsReturnsEmptyWhenNotStuck() {
        AsyncOpRegistry registry = new AsyncOpRegistry(100);
        registry.next((byte) 1);

        List<AsyncOpContext> stuck = registry.findStuckOps(1_000_000_000_000L);
        assertTrue(stuck.isEmpty());
    }

    @Test
    void findStuckOpsFindsOldOps() throws Exception {
        AsyncOpRegistry registry = new AsyncOpRegistry(100);
        AsyncOpContext ctx = registry.next((byte) 1);

        Thread.sleep(10);

        List<AsyncOpContext> stuck = registry.findStuckOps(1_000_000L);
        assertEquals(1, stuck.size());
        assertSame(ctx, stuck.get(0));
    }

    @Test
    void findStuckOpsOnlyReturnsInUse() throws Exception {
        AsyncOpRegistry registry = new AsyncOpRegistry(100);
        AsyncOpContext ctx = registry.next((byte) 1);

        Thread.sleep(10);
        ctx.inUse = false;

        List<AsyncOpContext> stuck = registry.findStuckOps(1_000_000L);
        assertTrue(stuck.isEmpty());
    }

    @Test
    void findStuckOpsReturnsMultiple() throws Exception {
        AsyncOpRegistry registry = new AsyncOpRegistry(100);
        AsyncOpContext ctx1 = registry.next((byte) 1);
        AsyncOpContext ctx2 = registry.next((byte) 2);

        Thread.sleep(10);

        List<AsyncOpContext> stuck = registry.findStuckOps(1_000_000L);
        assertEquals(2, stuck.size());
        assertTrue(stuck.contains(ctx1));
        assertTrue(stuck.contains(ctx2));
    }

    @Test
    void iteratorEmptyWhenNoOps() {
        AsyncOpRegistry registry = new AsyncOpRegistry(100);
        Iterator<AsyncOpContext> iter = registry.iterator();
        assertFalse(iter.hasNext());
    }

    @Test
    void iteratorReturnsInUseOnly() {
        AsyncOpRegistry registry = new AsyncOpRegistry(100);
        AsyncOpContext ctx1 = registry.next((byte) 1);
        AsyncOpContext ctx2 = registry.next((byte) 2);

        ctx1.inUse = false;

        List<AsyncOpContext> found = new ArrayList<>();
        for (AsyncOpContext ctx : registry) {
            found.add(ctx);
        }

        assertEquals(1, found.size());
        assertSame(ctx2, found.get(0));
    }

    @Test
    void iteratorReturnsAll() {
        AsyncOpRegistry registry = new AsyncOpRegistry(100);
        Set<AsyncOpContext> created = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            created.add(registry.next((byte) 1));
        }

        Set<AsyncOpContext> found = new HashSet<>();
        for (AsyncOpContext ctx : registry) {
            found.add(ctx);
        }

        assertEquals(created, found);
    }

    @Test
    void iteratorNextThrowsWhenExhausted() {
        AsyncOpRegistry registry = new AsyncOpRegistry(100);
        Iterator<AsyncOpContext> iter = registry.iterator();
        assertThrows(NoSuchElementException.class, iter::next);
    }

    @Test
    void iteratorNextAfterHasNextFalse() {
        AsyncOpRegistry registry = new AsyncOpRegistry(100);
        registry.next((byte) 1);

        Iterator<AsyncOpContext> iter = registry.iterator();
        iter.next();
        assertFalse(iter.hasNext());
        assertThrows(NoSuchElementException.class, iter::next);
    }

    @Test
    void idMappingUsesShortMinValue() {
        AsyncOpRegistry registry = new AsyncOpRegistry(10);
        AsyncOpContext ctx = registry.next((byte) 1);
        assertTrue(ctx.id < Short.MIN_VALUE + 10);
    }
}
