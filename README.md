# MongoDB Sequences - The Java Way

Code for the post [MongoDB sequences (autoincrement) - The Java Way](https://banach.net.pl/posts/2022/mongodb-sequences-autoincrement-the-java-way/).

A simple code to create sequence-like feature in MongoDB. Repository contains:
* `Sequence` record (mapped to document in MongoDB collection),
* ready to use `SequenceDAO`,
* some fancy tests in `SequenceDaoTest` using [TestContainers](https://www.testcontainers.org/)


To run tests, due to usage of TestContainers, Docker is required.