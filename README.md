# Kafka-HWE

## Studio Instructions

### Describe the topic "question-1"

A topic exists called "question-1". Use the command line to describe the topic "question-1":

- How many partitions does it have?
- What is the replication factor?

### Create a topic "question-2"

Using the Kafka CLI, create a topic with 3 partitions and a replication factor of 2. Call the topic "
question-2-yourname".

### Consume from "question-3"

A topic already exists called "question-3". Using `SimpleConsumer`, what are the first ten messages at the beginning of
this topic?

### Produce to "question-4"

Using `SimpleProducer`, produce your name as a string to the topic "question-4". Verify it is there by
using `SimpleConsumer` to read from the "question-4" topic

### Consume from "question-5"

A topic exists called "question-5" with JSON strings as its messages. Using `JSONConsumer`,

- Print out the string to understand the structure.
- Create a case class that matches this data structure.
- Parse the JSON string into the scala case class that you created.

### Stretch #1 - Build a system with a partner

Find a partner who is also on the stretch portion.

- Partner A:
    - Choose a topic name ( e.g. my-fake-)
- Publish Partner B:

### Stretch #2 - Theory

What is the CAP theorem, and where does Kafka fit within it?