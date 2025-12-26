package io.jiouring.file;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class SyscallFutureTest {

    @Test
    void completeWithPositiveResult() throws IOException {
        SyscallFuture future = new SyscallFuture((byte) 1);
        future.complete(42);
        assertEquals(42, future.join());
    }

    @Test
    void completeWithZeroResult() throws IOException {
        SyscallFuture future = new SyscallFuture((byte) 1);
        future.complete(0);
        assertEquals(0, future.join());
    }

    @Test
    void failWithIOException() {
        SyscallFuture future = new SyscallFuture((byte) 1);
        IOException cause = new IOException("test");
        future.fail(cause);
        IOException thrown = assertThrows(IOException.class, future::join);
        assertSame(cause, thrown);
    }

    @Test
    void failWithRuntimeException() {
        SyscallFuture future = new SyscallFuture((byte) 1);
        RuntimeException cause = new RuntimeException("test");
        future.fail(cause);
        RuntimeException thrown = assertThrows(RuntimeException.class, future::join);
        assertSame(cause, thrown);
    }

    @Test
    void failWithCheckedExceptionWrapsInIOException() {
        SyscallFuture future = new SyscallFuture((byte) 1);
        Exception cause = new Exception("test");
        future.fail(cause);
        IOException thrown = assertThrows(IOException.class, future::join);
        assertSame(cause, thrown.getCause());
    }

    @Test
    void failWithErrorWrapsInIOException() {
        SyscallFuture future = new SyscallFuture((byte) 1);
        Error cause = new Error("test");
        future.fail(cause);
        IOException thrown = assertThrows(IOException.class, future::join);
        assertSame(cause, thrown.getCause());
    }

    @Test
    void failSetsResultToMinusOne() throws Exception {
        SyscallFuture future = new SyscallFuture((byte) 1);
        future.fail(new RuntimeException());
        try {
            future.join();
            fail("Expected exception");
        } catch (RuntimeException ignored) {}
    }

    @Test
    void doubleCompleteThrows() {
        SyscallFuture future = new SyscallFuture((byte) 1);
        future.complete(1);
        assertThrows(IllegalStateException.class, () -> future.complete(2));
    }

    @Test
    void doubleFailThrows() {
        SyscallFuture future = new SyscallFuture((byte) 1);
        future.fail(new RuntimeException());
        assertThrows(IllegalStateException.class, () -> future.fail(new RuntimeException()));
    }

    @Test
    void completeAfterFailThrows() {
        SyscallFuture future = new SyscallFuture((byte) 1);
        future.fail(new RuntimeException());
        assertThrows(IllegalStateException.class, () -> future.complete(1));
    }

    @Test
    void failAfterCompleteThrows() {
        SyscallFuture future = new SyscallFuture((byte) 1);
        future.complete(1);
        assertThrows(IllegalStateException.class, () -> future.fail(new RuntimeException()));
    }

    @Test
    void onCompleteCalledImmediatelyIfDone() {
        SyscallFuture future = new SyscallFuture((byte) 1);
        future.complete(42);

        AtomicInteger result = new AtomicInteger();
        future.onComplete((res, ex) -> result.set(res));
        assertEquals(42, result.get());
    }

    @Test
    void onCompleteCalledWithExceptionIfFailed() {
        SyscallFuture future = new SyscallFuture((byte) 1);
        RuntimeException cause = new RuntimeException();
        future.fail(cause);

        AtomicReference<Throwable> captured = new AtomicReference<>();
        future.onComplete((res, ex) -> captured.set(ex));
        assertSame(cause, captured.get());
    }

    @Test
    void onCompleteCalledOnCompletion() {
        SyscallFuture future = new SyscallFuture((byte) 1);

        AtomicInteger result = new AtomicInteger(-999);
        future.onComplete((res, ex) -> result.set(res));

        assertEquals(-999, result.get());
        future.complete(100);
        assertEquals(100, result.get());
    }

    @Test
    void onCompleteCalledOnFailure() {
        SyscallFuture future = new SyscallFuture((byte) 1);

        AtomicReference<Throwable> captured = new AtomicReference<>();
        future.onComplete((res, ex) -> captured.set(ex));

        assertNull(captured.get());
        IOException cause = new IOException();
        future.fail(cause);
        assertSame(cause, captured.get());
    }

    @Test
    void multipleOnCompleteThrows() {
        SyscallFuture future = new SyscallFuture((byte) 1);
        future.onComplete((res, ex) -> {});
        assertThrows(IllegalStateException.class, () -> future.onComplete((res, ex) -> {}));
    }

    @Test
    void onCompleteAfterDoneInvokesImmediately() {
        SyscallFuture future = new SyscallFuture((byte) 1);
        future.onComplete((res, ex) -> {});
        future.complete(42);

        AtomicInteger result = new AtomicInteger();
        future.onComplete((res, ex) -> result.set(res));
        assertEquals(42, result.get());
    }

    @Test
    void joinReturnsImmediatelyIfDone() throws IOException {
        SyscallFuture future = new SyscallFuture((byte) 1);
        future.complete(42);
        assertEquals(42, future.join());
    }

    @Test
    @Timeout(5)
    void joinBlocksUntilComplete() throws Exception {
        SyscallFuture future = new SyscallFuture((byte) 1);

        CountDownLatch started = new CountDownLatch(1);
        AtomicInteger result = new AtomicInteger();

        Thread t = new Thread(() -> {
            started.countDown();
            try {
                result.set(future.join());
            } catch (IOException ignored) {}
        });
        t.start();

        started.await();
        Thread.sleep(50);
        future.complete(42);
        t.join(1000);

        assertFalse(t.isAlive());
        assertEquals(42, result.get());
    }

    @Test
    @Timeout(5)
    void joinUnblocksOnFailure() throws Exception {
        SyscallFuture future = new SyscallFuture((byte) 1);

        CountDownLatch started = new CountDownLatch(1);
        AtomicReference<Throwable> caught = new AtomicReference<>();

        Thread t = new Thread(() -> {
            started.countDown();
            try {
                future.join();
            } catch (Throwable ex) {
                caught.set(ex);
            }
        });
        t.start();

        started.await();
        Thread.sleep(50);
        future.fail(new IOException("expected"));
        t.join(1000);

        assertFalse(t.isAlive());
        assertInstanceOf(IOException.class, caught.get());
    }

    @Test
    void handlerExceptionDoesNotPreventUnpark() throws Exception {
        SyscallFuture future = new SyscallFuture((byte) 1);
        future.onComplete((r, e) -> {
            throw new RuntimeException("handler crash");
        });

        CountDownLatch started = new CountDownLatch(1);
        AtomicInteger result = new AtomicInteger();

        Thread t = new Thread(() -> {
            started.countDown();
            try {
                result.set(future.join());
            } catch (IOException ignored) {}
        });
        t.start();
        started.await();
        Thread.sleep(50);

        future.complete(42);
        t.join(1000);

        assertFalse(t.isAlive());
        assertEquals(42, result.get());
    }

}
