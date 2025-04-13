![schema_messenger](https://github.com/user-attachments/assets/2c51f515-ca60-46ee-ab29-ccbe76b98666)
We can shard our database based on the user_id

## Keyspace Configuration

```cql
CREATE KEYSPACE IF NOT EXISTS messenger
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 3
};
```

In a production environment, we would use `NetworkTopologyStrategy` to specify replication factors for each data center.

## Table Designs

### 1. Messages Table

This table stores all messages sent between users. It uses a composite partition key to efficiently fetch messages in a conversation.

```cql
CREATE TABLE IF NOT EXISTS messages (
    conversation_id INT,
    message_id TIMEUUID,  
    sender_id INT,
    receiver_id INT,
    content TEXT,
    created_at TIMESTAMP,
    PRIMARY KEY ((conversation_id), created_at, message_id)
) WITH CLUSTERING ORDER BY (created_at DESC, message_id DESC);
```

#### Purpose and Data:
- Primary storage for all message content
- Each message belongs to exactly one conversation
- Messages are partitioned by conversation for efficient retrieval
- The descending order allows fetching the most recent messages first
- Typical queries fetch 20-50 messages at a time for display

#### Column Details:
- `conversation_id`: The partition key, ensures all messages in a conversation are stored together
- `message_id`: A TimeUUID that provides both uniqueness and timestamp information
- `created_at`: Explicit timestamp for message ordering and filtering
- `sender_id`: ID of the user who sent the message
- `receiver_id`: ID of the user who receives the message
- `content`: The actual text content of the message

### 2. Conversations by User Table

This table efficiently retrieves all conversations for a user, ordered by the most recent activity.

```cql
CREATE TABLE IF NOT EXISTS conversations_by_user (
    user_id INT,
    conversation_id INT,
    other_user_id INT,
    last_message_at TIMESTAMP,
    last_message_content TEXT,
    PRIMARY KEY ((user_id), last_message_at, conversation_id)
) WITH CLUSTERING ORDER BY (last_message_at DESC, conversation_id ASC);
```

#### Purpose and Data:
- This is an index table that allows efficient retrieval of a user's conversations
- Each row represents one conversation from a specific user's perspective
- Optimized for the "conversation list" view in messaging apps
- Duplicate data across users enables fast reads without joins

#### Column Details:
- `user_id`: The partition key, enables efficient retrieval for a specific user
- `last_message_at`: Clustering column allowing sorting by most recent activity
- `conversation_id`: Additional clustering column for uniqueness
- `other_user_id`: The ID of the other participant in the conversation
- `last_message_content`: Preview of the most recent message (for display in conversation lists)

### 3. Conversations Table

This table stores the details of each conversation.

```cql
CREATE TABLE IF NOT EXISTS conversations (
    conversation_id INT PRIMARY KEY,
    user1_id INT,
    user2_id INT,
    created_at TIMESTAMP,
    last_message_at TIMESTAMP,
    last_message_content TEXT
);
```

#### Purpose and Data:
- Stores the metadata about conversations
- Provides a single source of truth for conversation details
- Used for direct lookups by conversation_id
- Also stores the latest message details for optimization

#### Column Details:
- `conversation_id`: Primary key for efficient lookups
- `user1_id`: The ID of the first participant (always the lower user ID)
- `user2_id`: The ID of the second participant (always the higher user ID)
- `created_at`: When the conversation was created
- `last_message_at`: Timestamp of the most recent message
- `last_message_content`: Content of the most recent message

## UUID Handling

For message IDs, we use the TIMEUUID type in Cassandra, which is a Version 1 UUID that includes:
- A timestamp component (first 8 bytes)
- A node identifier (last 6 bytes)
- Other unique identifiers

When sending messages to clients, we convert these UUIDs to strings, providing globally unique identifiers that also preserve time ordering.

## Data Flow and Consistency

### When a Message is Sent:
1. A new row is inserted into the `messages` table
2. The `conversations` table is updated with the latest message details
3. Both users' entries in the `conversations_by_user` table are updated

This approach ensures that:
- Message content is stored once only (in the `messages` table)
- Conversation lists for both users are updated immediately
- The latest message preview is available in conversation listings

## Query Patterns

1. **Send a message**:
   - Insert into `messages` table with a TIMEUUID for message_id
   - Update `conversations_by_user` table for both users with new last_message details
   - Update `conversations` table with new last_message details

2. **Get conversations for a user**:
   - Query `conversations_by_user` with the user_id
   - Paginate using `last_message_at` and `conversation_id`

3. **Get messages in a conversation**:
   - Query `messages` table with the conversation_id
   - Paginate using `created_at` and `message_id`

4. **Get messages before a timestamp**:
   - Query `messages` table with the conversation_id
   - Add condition `created_at < timestamp`
   - Paginate using `created_at` and `message_id`

5. **Get a specific conversation by ID**:
   - Direct lookup in the `conversations` table using conversation_id

## Design Considerations

1. **Denormalization**: The schema uses denormalization to optimize for read performance, which is critical for a messaging application. Data is duplicated across tables to avoid costly joins.

2. **Pagination**: Uses a combination of timestamp and ID for efficient pagination, ensuring consistent ordering even when messages have identical timestamps.

3. **Data Distribution**: Partition keys are designed to distribute data evenly across the cluster while keeping related data together.

4. **Efficient Queries**: All required API operations are supported with efficient queries without the need for secondary indexes or ALLOW FILTERING (except for conversation lookup by user IDs).

5. **Consistency**: For a messaging application, eventual consistency is acceptable for most operations, but we must ensure that message order is preserved within conversations.

6. **UUID Benefits**: Using TIMEUUIDs for message IDs provides both uniqueness and natural time ordering, making it ideal for a messaging application.

7. **Optimization**: Storing the last message details in both the `conversations` and `conversations_by_user` tables optimizes the common use case of displaying conversation lists with message previews.
