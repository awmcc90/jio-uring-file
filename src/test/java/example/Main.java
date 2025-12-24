package example;

import example.runners.BasicExample;
import example.runners.TransactionalJournal;

public class Main {
    public static void main(String[] args) throws Exception {
        TransactionalJournal.run().join();
        BasicExample.run();
    }
}
