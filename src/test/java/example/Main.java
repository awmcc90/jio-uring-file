package example;

import example.runners.TransactionalJournal;

public class Main {
    public static void main(String[] args) {
        TransactionalJournal.run().join();
    }
}
