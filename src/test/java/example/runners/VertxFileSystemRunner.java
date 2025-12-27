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

        Path path = Path.of("/tmp/random-file.dat");
        Files.deleteIfExists(path);

        OpenOptions options = new OpenOptions()
            .setCreateNew(true)
            .setRead(true)
            .setWrite(true);

        {
            AsyncFile file = vertx.fileSystem().open(path.toString(), options).await();
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
            long startTime= System.nanoTime();
            ContextInternal context = (ContextInternal) vertx.getOrCreateContext();
            logger.info("Event loop type: {}", context.nettyEventLoop().getClass().getSimpleName());
            IoUringFileIoHandle handle = IoUringFileIoHandle.open(Path.of("/tmp/bench_read16140423041667483984.dat"), (IoEventLoop) context.nettyEventLoop()).join();
            IoUringAsyncFileImpl asyncFile = new IoUringAsyncFileImpl((VertxInternal) vertx, context, handle);

            Promise<Void> promise = Promise.promise();
            context.execute(() -> {
                asyncFile.handler(buffer -> {});
                asyncFile.endHandler(v -> {
                    long endTime = System.nanoTime();
                    logger.info("[IoUringAsyncFileImpl] Finished reading. Took {}", formatNanos(endTime - startTime));
                    context.execute(() -> asyncFile.close().onComplete(ar -> {
                        if (ar.failed()) promise.fail(ar.cause());
                        else promise.complete();
                    }));
                });
                asyncFile.exceptionHandler(promise::fail);
            });
            promise.future().await();
            logger.info("Done IoUringAsyncFileImpl");
        }

        {
            long startTime= System.nanoTime();
            Promise<Void> readDone = Promise.promise();
            vertx
                .fileSystem()
                .open("/tmp/bench_read16140423041667483984.dat", new OpenOptions().setRead(true))
                .onComplete(ar -> {
                    if (ar.failed()) readDone.fail(ar.cause());
                    else {
                        AsyncFile file = ar.result();
                        file.handler(buffer -> {});
                        file.endHandler(v -> {
                            long endTime = System.nanoTime();
                            logger.info("Finished reading. Took {}", formatNanos(endTime - startTime));
                            file.close().onComplete(ignored -> readDone.complete());
                        });
                        file.exceptionHandler(readDone::fail);
                    }
                });

            readDone.future().await();
            logger.info("Done AsyncFileImpl");
        }

        Files.deleteIfExists(path);
        vertx.close().await(3, TimeUnit.SECONDS);
    }

    private static String formatNanos(long ns) {
        if (ns < 1_000) return ns + " ns";
        if (ns < 1_000_000) return (ns / 1_000.0) + " µs";
        if (ns < 1_000_000_000) return (ns / 1_000_000.0) + " ms";
        return (ns / 1_000_000_000.0) + " s";
    }
}
