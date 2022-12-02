package com.elwin013.mongoSequences;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class SequenceDaoTest {
    public static final String TEST_DATABASE_NAME = "test";
    private static final String TEST_SEQUENCE_NAME = "my_sequence";
    private static final CodecRegistry POJO_CODEC_REGISTRY =
            CodecRegistries.fromRegistries(
                    MongoClientSettings.getDefaultCodecRegistry(),
                    CodecRegistries.fromProviders(PojoCodecProvider.builder().automatic(true).build())
            );
    private MongoDBContainer mongoDBContainer;
    private MongoDatabase database;
    private MongoClient client;

    @BeforeEach
    public void beforeEach() {
        mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:5.0.14"));
        mongoDBContainer.start();

        client = MongoClients.create(mongoDBContainer.getConnectionString());
        database = client.getDatabase(TEST_DATABASE_NAME).withCodecRegistry(POJO_CODEC_REGISTRY);
    }

    @AfterEach
    public void afterEach() {
        client.getDatabase(TEST_DATABASE_NAME).drop();
        mongoDBContainer.stop();
    }

    @Test
    public void initialized_sequence_should_have_correct_value_in_database() {
        var dao = new SequenceDao(database);
        dao.reset(TEST_SEQUENCE_NAME, 2);

        var sequenceFromDatabase = database
                .getCollection(SequenceDao.COLLECTION_NAME, Sequence.class)
                .find(Filters.eq(TEST_SEQUENCE_NAME))
                .first();

        assertNotNull(sequenceFromDatabase);
        assertEquals(2, sequenceFromDatabase.value());
    }

    @Test
    public void accessed_but_not_existing_sequence_should_have_value_equal_to_one() {
        var dao = new SequenceDao(database);
        var value = dao.getNextValue(TEST_SEQUENCE_NAME);

        assertEquals(1, value);
    }

    @Test
    public void sequence_accessed_multiple_times_should_get_next_values() {
        var dao = new SequenceDao(database);
        for (int i = 1; i < 10; i++) {
            var value = dao.getNextValue(TEST_SEQUENCE_NAME);
            assertEquals(i, value);
        }
    }

    @Test
    public void sequence_concurrency_test() {
        var dao = new SequenceDao(database);
        // Init to 0 - next value will be 1
        dao.reset(TEST_SEQUENCE_NAME, 0);


        // Create executor with 50 threads and run 100k request to next value
        ExecutorService executor = Executors.newFixedThreadPool(50);
        List<CompletableFuture<Void>> tasks = new ArrayList<>();
        for (int i = 0; i < 100_000; i++) {
            tasks.add(CompletableFuture.runAsync(() -> dao.getNextValue(TEST_SEQUENCE_NAME), executor));
        }


        CompletableFuture.allOf(tasks.toArray(new CompletableFuture[0])).join();
        executor.shutdownNow();

        var sequence = database.getCollection(SequenceDao.COLLECTION_NAME, Sequence.class)
                .find(Filters.eq(TEST_SEQUENCE_NAME)).first();

        assertEquals(100_000, sequence.value());
    }

}