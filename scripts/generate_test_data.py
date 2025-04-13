"""
Script to generate test data for the Messenger application.
This script generates users, conversations between users, and messages within conversations.
"""
import os
import uuid
import logging
import random
from datetime import datetime, timedelta
from cassandra.cluster import Cluster

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cassandra connection settings
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
CASSANDRA_KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "messenger")

# Test data configuration
NUM_USERS = 10  # Number of users to create
NUM_CONVERSATIONS = 15  # Number of conversations to create
MAX_MESSAGES_PER_CONVERSATION = 50  # Maximum number of messages per conversation

def connect_to_cassandra():
    """Connect to Cassandra cluster."""
    logger.info("Connecting to Cassandra...")
    try:
        cluster = Cluster([CASSANDRA_HOST])
        session = cluster.connect(CASSANDRA_KEYSPACE)
        logger.info("Connected to Cassandra!")
        return cluster, session
    except Exception as e:
        logger.error(f"Failed to connect to Cassandra: {str(e)}")
        raise

def generate_conversation_id(user1_id, user2_id):
    """
    Generate a consistent conversation ID from two user IDs.
    This ensures we don't create duplicate conversations between the same users.
    """
    # Sort user IDs to maintain consistency
    lower_id = min(user1_id, user2_id)
    higher_id = max(user1_id, user2_id)
    
    # Generate conversation ID based on user IDs
    conversation_id = abs(hash(f"{lower_id}:{higher_id}") % 100000000)
    return conversation_id

def generate_test_data(session):
    """
    Generate test data in Cassandra.
    
    Creates:
    - Users (with IDs 1-NUM_USERS)
    - Conversations between random pairs of users
    - Messages in each conversation with realistic timestamps
    """
    logger.info("Generating test data...")
    
    # 1. Create a set of user IDs (1 to NUM_USERS)
    user_ids = list(range(1,NUM_USERS+1))
    logger.info(f"Created {len(user_ids)} users with IDs from 1 to {NUM_USERS}")
    
    # 2. Create conversations between random pairs of users
    conversations = []
    conversation_pairs = set()
    
    # Create NUM_CONVERSATIONS random conversations
    while len(conversations) < NUM_CONVERSATIONS:
        # Pick two different random users
        user1_id = random.choice(user_ids)
        user2_id = random.choice([u for u in user_ids if u != user1_id])
        
        # Ensure this pair doesn't already have a conversation
        user_pair = (min(user1_id, user2_id), max(user1_id, user2_id))
        if user_pair in conversation_pairs:
            continue
        
        # Add the pair to our set of conversation pairs
        conversation_pairs.add(user_pair)
        
        # Generate conversation ID
        conversation_id = generate_conversation_id(user1_id, user2_id)
        
        # Record conversation details for later use
        conversations.append({
            'id': conversation_id,
            'user1_id': min(user1_id, user2_id),
            'user2_id': max(user1_id, user2_id),
            'created_at': datetime.now() - timedelta(days=random.randint(1, 30))
        })
    
    logger.info(f"Generated {len(conversations)} conversations")
    
    # 3. Insert conversations into Cassandra tables
    for conversation in conversations:
        # Insert into conversations table
        conversation_query = """
            INSERT INTO conversations
            (conversation_id, user1_id, user2_id, created_at, last_message_at, last_message_content)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        session.execute(conversation_query, (
            conversation['id'], 
            conversation['user1_id'], 
            conversation['user2_id'], 
            conversation['created_at'],
            None,  # last_message_at will be updated when messages are added
            None   # last_message_content will be updated when messages are added
        ))
    
    # 4. For each conversation, generate a random number of messages
    message_count = 0
    message_samples = [
        "Hey, how are you?", "What's up?", "I'm good, thanks!", 
        "Did you see the new movie?", "Let's grab coffee sometime.",
        "Are you free this weekend?", "I'll get back to you soon.",
        "Can you help me with something?", "Have a great day!",
        "Sorry, I was busy.", "Sure thing!", "Maybe later?",
        "That's awesome!", "I don't think so.", "Absolutely!",
        "That's interesting.", "I'll be there.", "Don't worry about it.",
        "Thanks for the update!", "Let me check and get back to you.", 
        "Sounds like a plan!", "Good idea!", "I miss you!",
        "Just checking in.", "See you soon!", "Call me when you can.",
        "What time works for you?", "I'm running late.", "No problem!",
        "Let me know what you think.", "Can't wait!", "That's hilarious!",
        "Good morning!", "Good night!", "Have a nice weekend!",
        "Happy birthday!", "Congratulations!", "I'm sorry to hear that.",
        "That's great news!", "I'm excited!", "Let's do this again sometime.",
        "What do you think?", "I agree with you.", "That's amazing!",
        "I'll be right back.", "Take your time.", "I understand.",
        "No worries!", "Perfect!", "Sounds good!"
    ]
    
    for conversation in conversations:
        # Decide how many messages this conversation will have
        num_messages = random.randint(3, MAX_MESSAGES_PER_CONVERSATION)
        
        # Track the most recent message for updating conversation metadata
        latest_message_time = None
        latest_message_content = None
        
        # Generate messages with timestamps in ascending order
        message_timestamps = []
        start_time = conversation['created_at']
        end_time = datetime.now() - timedelta(minutes=random.randint(5, 60))
        
        for i in range(num_messages):
            # Create a realistic progression of timestamps
            if i == 0:
                timestamp = start_time + timedelta(minutes=random.randint(5, 30))
            elif i == num_messages - 1:
                timestamp = end_time
            else:
                # Distribute messages between start and end
                progress = i / (num_messages - 1)
                time_range = (end_time - start_time).total_seconds()
                seconds_from_start = time_range * progress
                variation = random.uniform(-0.1, 0.1) * time_range  # Add some randomness
                timestamp = start_time + timedelta(seconds=seconds_from_start + variation)
            
            message_timestamps.append(timestamp)
        
        # Sort timestamps to ensure chronological order
        message_timestamps.sort()
        
        # Generate and insert messages
        for i, timestamp in enumerate(message_timestamps):
            # Decide who sends this message
            if random.random() < 0.5:
                sender_id = conversation['user1_id']
                receiver_id = conversation['user2_id']
            else:
                sender_id = conversation['user2_id']
                receiver_id = conversation['user1_id']
            
            # Generate message content
            content = random.choice(message_samples)
            
            # Generate TimeUUID for the message
            message_id = uuid.uuid1()
            
            # Insert the message
            message_query = """
                INSERT INTO messages 
                (conversation_id, message_id, sender_id, receiver_id, content, created_at) 
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            session.execute(message_query, (
                conversation['id'],
                message_id,
                sender_id,
                receiver_id,
                content,
                timestamp
            ))
            
            message_count += 1
            
            # Keep track of the latest message for updating conversation metadata
            if latest_message_time is None or timestamp > latest_message_time:
                latest_message_time = timestamp
                latest_message_content = content
        
        # Update conversations table with the latest message details
        update_conv_query = """
            UPDATE conversations
            SET last_message_at = %s, last_message_content = %s
            WHERE conversation_id = %s
        """
        session.execute(update_conv_query, (
            latest_message_time, 
            latest_message_content, 
            conversation['id']
        ))
        
        # Update conversations_by_user for both users
        for user_id, other_user_id in [
            (conversation['user1_id'], conversation['user2_id']),
            (conversation['user2_id'], conversation['user1_id'])
        ]:
            user_conv_query = """
                INSERT INTO conversations_by_user 
                (user_id, conversation_id, other_user_id, last_message_at, last_message_content) 
                VALUES (%s, %s, %s, %s, %s)
            """
            session.execute(user_conv_query, (
                user_id,
                conversation['id'],
                other_user_id,
                latest_message_time,
                latest_message_content
            ))
    logger.info(f"These are the all conversation_ids : {[conv['id'] for conv in conversations]}")
    logger.info(f"These are all the conversation users pairs : {[(conv['user1_id'], conv['user2_id']) for conv in conversations]}")
    logger.info(f"Generated {message_count} messages across {len(conversations)} conversations")
    logger.info(f"User IDs range from 1 to {NUM_USERS}")
    logger.info("Use these IDs for testing the API endpoints")

def main():
    """Main function to generate test data."""
    cluster = None
    
    try:
        # Connect to Cassandra
        cluster, session = connect_to_cassandra()
        
        # Generate test data
        generate_test_data(session)
        
        logger.info("Test data generation completed successfully!")
    except Exception as e:
        logger.error(f"Error generating test data: {str(e)}")
    finally:
        if cluster:
            cluster.shutdown()
            logger.info("Cassandra connection closed")

if __name__ == "__main__":
    main() 