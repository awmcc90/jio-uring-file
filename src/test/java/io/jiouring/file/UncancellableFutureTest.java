package io.jiouring.file;

import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;

class UncancellableFutureTest {

    @Test
    void cancelReturnsFalse() {
        UncancellableFuture<String> future = new UncancellableFuture<>();
        assertFalse(future.cancel(true));
        assertFalse(future.cancel(false));
    }

    @Test
    void cancelDoesNotPreventCompletion() throws Exception {
        UncancellableFuture<String> future = new UncancellableFuture<>();
        future.cancel(true);
        future.complete("value");
        assertEquals("value", future.get());
    }

    @Test
    void cancelDoesNotMarkAsCancelled() {
        UncancellableFuture<String> future = new UncancellableFuture<>();
        future.cancel(true);
        assertFalse(future.isCancelled());
    }

    @Test
    void obtrudeValueThrows() {
        UncancellableFuture<String> future = new UncancellableFuture<>();
        assertThrows(UnsupportedOperationException.class, () -> future.obtrudeValue("x"));
    }

    @Test
    void obtrudeValueThrowsEvenAfterCompletion() {
        UncancellableFuture<String> future = new UncancellableFuture<>();
        future.complete("done");
        assertThrows(UnsupportedOperationException.class, () -> future.obtrudeValue("x"));
    }

    @Test
    void obtrudeExceptionThrows() {
        UncancellableFuture<String> future = new UncancellableFuture<>();
        assertThrows(UnsupportedOperationException.class,
            () -> future.obtrudeException(new RuntimeException()));
    }

    @Test
    void obtrudeExceptionThrowsEvenAfterCompletion() {
        UncancellableFuture<String> future = new UncancellableFuture<>();
        future.complete("done");
        assertThrows(UnsupportedOperationException.class,
            () -> future.obtrudeException(new RuntimeException()));
    }

    @Test
    void normalCompletionWorks() throws Exception {
        UncancellableFuture<Integer> future = new UncancellableFuture<>();
        future.complete(42);
        assertTrue(future.isDone());
        assertEquals(42, future.get());
    }

    @Test
    void exceptionalCompletionWorks() {
        UncancellableFuture<String> future = new UncancellableFuture<>();
        RuntimeException ex = new RuntimeException("test");
        future.completeExceptionally(ex);
        assertTrue(future.isDone());
        assertTrue(future.isCompletedExceptionally());
        ExecutionException thrown = assertThrows(ExecutionException.class, future::get);
        assertSame(ex, thrown.getCause());
    }

    @Test
    void nullValueCompletion() throws Exception {
        UncancellableFuture<String> future = new UncancellableFuture<>();
        future.complete(null);
        assertTrue(future.isDone());
        assertNull(future.get());
    }

    @Test
    void multipleCancelAttemptsAllReturnFalse() {
        UncancellableFuture<String> future = new UncancellableFuture<>();
        for (int i = 0; i < 10; i++) {
            assertFalse(future.cancel(true));
            assertFalse(future.cancel(false));
        }
        assertFalse(future.isCancelled());
    }

    @Test
    void cancelAfterCompletionReturnsFalse() {
        UncancellableFuture<String> future = new UncancellableFuture<>();
        future.complete("done");
        assertFalse(future.cancel(true));
        assertFalse(future.isCancelled());
    }

    @Test
    void thenApplyWorksNormally() throws Exception {
        UncancellableFuture<Integer> future = new UncancellableFuture<>();
        var mapped = future.thenApply(x -> x * 2);
        future.complete(21);
        assertEquals(42, mapped.get());
    }
}
