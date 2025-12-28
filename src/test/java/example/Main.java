package example;

import example.runners.BasicExample;
import example.runners.Jio;
import example.runners.TransactionalJournal;
import example.runners.VertxFileSystemRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class.getName());

    public static void main(String[] args) {
        Runnable[] runnables = new Runnable[] {
            new Jio(),
            new VertxFileSystemRunner(),
            //new TransactionalJournal(),
            new BasicExample(),
        };

        for (Runnable runnable : runnables) {
            logger.info("Running {}", runnable.getClass().getSimpleName());
            runnable.run();
            logger.info("Done {}\n\n", runnable.getClass().getSimpleName());
        }
    }
}
