package com.elwin013.mongoSequences;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.ReturnDocument;
import com.mongodb.client.model.Updates;

public final class SequenceDao {
    final static String COLLECTION_NAME = "sequence";
    private final static FindOneAndUpdateOptions UPSERT = new FindOneAndUpdateOptions().upsert(true).returnDocument(ReturnDocument.AFTER);
    private final MongoDatabase db;

    public SequenceDao(MongoDatabase db) {
        this.db = db;
    }

    public long getNextValue(String sequenceName) {
        var seq = db.getCollection(COLLECTION_NAME, Sequence.class)
                .findOneAndUpdate(
                        Filters.eq(sequenceName),
                        Updates.inc("value", 1),
                        UPSERT
                );

        return seq.value();
    }

    public long reset(String sequenceName, long value) {
        var seq = db.getCollection(COLLECTION_NAME, Sequence.class)
                .findOneAndUpdate(
                        Filters.eq(sequenceName),
                        Updates.set("value", value),
                        UPSERT
                );

        return seq.value();
    }
}
