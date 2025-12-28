package example.runners;

import example.vertx.IoUringAsyncFileImpl;
import io.jiouring.file.IoUringFileIoHandle;
import io.netty.channel.IoEventLoop;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.internal.ContextInternal;
import io.vertx.core.internal.VertxInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

public class VertxFileSystemRunner implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(VertxFileSystemRunner.class);

    @Override
    public void run() {
        try {
            runInternal();
        } catch (IOException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }

    private static void runInternal() throws IOException, TimeoutException {
        Vertx vertx = Vertx.vertx(
            new VertxOptions()
                .setPreferNativeTransport(true)
        );

        ContextInternal context = (ContextInternal) vertx.getOrCreateContext();

        Path largeFilePath = Path.of("/tmp/bench_read16140423041667483984.dat");

        Path path = Path.of("/tmp/random-file.dat");
        Files.deleteIfExists(path);

        {
            AsyncFile file = vertx.fileSystem().open(path.toString(), new OpenOptions().setCreateNew(true).setRead(true).setWrite(true)).await();
            logger.info("File opened: {}", path);

            Buffer writeBuffer = Buffer.buffer("Hello World!".getBytes());
            file.write(writeBuffer).await();
            logger.info("Wrote data to file: {}", writeBuffer);

            Buffer readBuffer = Buffer.buffer(writeBuffer.length());
            logger.info("Read buffer length: {} ({})", readBuffer.length(), writeBuffer.length());

            file.read(readBuffer, 0, 0, writeBuffer.length()).await();
            logger.info("Read data from file: {}", readBuffer);

            file.close().await();
        }

        {
            long startTime = System.nanoTime();
            IoUringFileIoHandle handle = IoUringFileIoHandle.open(largeFilePath, (IoEventLoop) context.nettyEventLoop()).join();
            IoUringAsyncFileImpl asyncFile = new IoUringAsyncFileImpl((VertxInternal) vertx, context, handle);
            Promise<Void> promise = Promise.promise();
            context.execute(() -> {
                AtomicLong counter = new AtomicLong(0);
                asyncFile.handler(buffer -> counter.addAndGet(buffer.length()));
                asyncFile.endHandler(v -> {
                    long endTime = System.nanoTime();
                    logger.info("[IoUringAsyncFileImpl] Finished reading {} bytes. Took {}", counter.get(), formatNanos(endTime - startTime));
                    context.execute(() -> asyncFile.close()
                        .onComplete(ar -> {
                            if (ar.failed()) promise.fail(ar.cause());
                            else promise.complete();
                        }));
                });
                asyncFile.exceptionHandler(promise::fail);
            });
            promise.future().await();
            logger.info("Done IoUringAsyncFileImpl reading.");
        }

        {
            Path writePath = Path.of("io_uring_copy.dat");
            Files.deleteIfExists(writePath);

            IoUringFileIoHandle readHandle = IoUringFileIoHandle.open(largeFilePath, (IoEventLoop) context.nettyEventLoop()).join();
            IoUringAsyncFileImpl readFile = new IoUringAsyncFileImpl((VertxInternal) vertx, context, readHandle);

            IoUringFileIoHandle writeHandle = IoUringFileIoHandle.open(writePath, (IoEventLoop) context.nettyEventLoop(), READ, WRITE, CREATE_NEW).join();
            IoUringAsyncFileImpl writeFile = new IoUringAsyncFileImpl((VertxInternal) vertx, context, writeHandle);

            long startTime = System.nanoTime();

            Promise<Void> promise = Promise.promise();
            context.execute(() -> readFile.pipeTo(writeFile)
                .onComplete(ar -> {
                    long endTime = System.nanoTime();
                    logger.info("[IoUringAsyncFileImpl] Finished pipe: {}", formatNanos(endTime - startTime));

                    if (ar.failed()) promise.fail(ar.cause());
                    else promise.complete();
                }));
            promise.future().await();
            logger.info("Done IoUringAsyncFileImpl pipe.");
        }

        {
            long startTime = System.nanoTime();
            Promise<Void> readDone = Promise.promise();
            vertx
                .fileSystem()
                .open(largeFilePath.toString(), new OpenOptions().setRead(true))
                .onComplete(ar -> {
                    if (ar.failed()) readDone.fail(ar.cause());
                    else {
                        AsyncFile file = ar.result();
                        AtomicLong counter = new AtomicLong(0);
                        file.handler(buffer -> counter.addAndGet(buffer.length()));
                        file.endHandler(v -> {
                            long endTime = System.nanoTime();
                            logger.info("[AsyncFileImpl] Finished reading {} bytes. Took {}", counter.get(), formatNanos(endTime - startTime));
                            file.close().onComplete(ignored -> readDone.complete());
                        });
                        file.exceptionHandler(readDone::fail);
                    }
                });

            readDone.future().await();
            logger.info("Done AsyncFileImpl reading.");
        }

        {
            Path writePath = Path.of("async_file_copy.dat");
            Files.deleteIfExists(writePath);

            AsyncFile readFile = vertx.fileSystem().openBlocking(largeFilePath.toString(), new OpenOptions().setRead(true));
            AsyncFile writeFile = vertx.fileSystem().openBlocking(writePath.toString(), new OpenOptions().setRead(true).setWrite(true).setCreateNew(true));

            long startTime = System.nanoTime();

            Promise<Void> promise = Promise.promise();
            context.execute(() -> readFile.pipeTo(writeFile)
                .onComplete(ar -> {
                    long endTime = System.nanoTime();
                    logger.info("[AsyncFileImpl] Finished pipe: {}", formatNanos(endTime - startTime));

                    if (ar.failed()) promise.fail(ar.cause());
                    else promise.complete();
                }));
            promise.future().await();
            logger.info("Done AsyncFileImpl pipe.");
        }

        Files.deleteIfExists(path);
        vertx.close().await(5, TimeUnit.SECONDS);
    }

    private static String formatNanos(long ns) {
        if (ns < 1_000) return ns + " ns";
        if (ns < 1_000_000) return (ns / 1_000.0) + " µs";
        if (ns < 1_000_000_000) return (ns / 1_000_000.0) + " ms";
        return (ns / 1_000_000_000.0) + " s";
    }
}
