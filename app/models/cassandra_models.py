"""
Models for interacting with Cassandra tables.
"""
import uuid
import time
import random
from datetime import datetime
from typing import List, Dict, Any, Optional

from app.db.cassandra_client import cassandra_client

class MessageModel:
    """
    Message model for interacting with the messages table.
    """
    
    @staticmethod
    async def create_message(sender_id: int, receiver_id: int, content: str) -> Dict[str, Any]:
        """
        Create a new message and update conversation records.
        
        Args:
            sender_id: ID of the message sender
            receiver_id: ID of the message receiver
            content: Message content text
            
        Returns:
            Dictionary with message details
        """
        # Generate a TimeUUID for the message (provides both unique ID and timestamp)
        message_id = uuid.uuid1()
        created_at = datetime.now()  # Use current time for created_at
        
        # Get or create conversation
        conversation = await ConversationModel.create_or_get_conversation(
            user1_id=sender_id,
            user2_id=receiver_id
        )
        
        conversation_id = conversation["id"]
        
        # Insert the message into messages table
        query = """
            INSERT INTO messages 
            (conversation_id, message_id, sender_id, receiver_id, content, created_at) 
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        cassandra_client.execute(query, 
            (conversation_id, message_id, sender_id, receiver_id, content, created_at)
        )
        
        # Update the conversations table with the latest message details
        update_conv_query = """
            UPDATE conversations
            SET last_message_at = %s, last_message_content = %s
            WHERE conversation_id = %s
        """
        cassandra_client.execute(update_conv_query, 
            (created_at, content, conversation_id)
        )
        
        # Update conversations_by_user for sender
        await MessageModel._update_conversation_for_user(
            user_id=sender_id,
            other_user_id=receiver_id,
            conversation_id=conversation_id,
            last_message_at=created_at,
            last_message_content=content
        )
        
        # Update conversations_by_user for receiver
        await MessageModel._update_conversation_for_user(
            user_id=receiver_id,
            other_user_id=sender_id,
            conversation_id=conversation_id,
            last_message_at=created_at,
            last_message_content=content
        )
        
        # Return the created message with UUID as string
        return {
            "id": str(message_id),  # Convert UUID to string
            "sender_id": sender_id,
            "receiver_id": receiver_id,
            "content": content,
            "created_at": created_at,
            "conversation_id": conversation_id
        }
    
    @staticmethod
    async def _update_conversation_for_user(
        user_id: int,
        other_user_id: int,
        conversation_id: int,
        last_message_at: datetime,
        last_message_content: str
    ) -> None:
        """
        Update conversation record for a user.
        
        Args:
            user_id: ID of the user to update the conversation for
            other_user_id: ID of the other participant in the conversation
            conversation_id: ID of the conversation
            last_message_at: Timestamp of the last message
            last_message_content: Content of the last message
        """
        query = """
            INSERT INTO conversations_by_user 
            (user_id, conversation_id, other_user_id, last_message_at, last_message_content) 
            VALUES (%s, %s, %s, %s, %s)
        """
        
        # Execute query without await - Cassandra client execute is not actually async
        cassandra_client.execute(query, 
            (user_id, conversation_id, other_user_id, last_message_at, last_message_content)
        )
    
    @staticmethod
    async def get_conversation_messages(
        conversation_id: int,
        page: int = 1,
        limit: int = 20
    ) -> Dict[str, Any]:
        """
        Get messages for a conversation with pagination.
        
        Args:
            conversation_id: ID of the conversation
            page: Page number (1-indexed)
            limit: Maximum number of messages per page
            
        Returns:
            Dictionary with paginated results
        """
        # Calculate pagination offset
        offset = (page - 1) * limit
        
        # Query for messages
        query = """
            SELECT message_id, sender_id, receiver_id, content, created_at, conversation_id 
            FROM messages 
            WHERE conversation_id = %s 
            LIMIT %s
        """
        
        result = cassandra_client.execute(query, (conversation_id, limit + offset))
        
        # Apply offset and limit
        messages = result[offset:offset+limit]
        
        # Format messages to match our schema (with string IDs from UUIDs)
        formatted_messages = []
        for message in messages:
            formatted_messages.append({
                "id": str(message["message_id"]),  # Convert UUID to string
                "sender_id": message["sender_id"],
                "receiver_id": message["receiver_id"],
                "content": message["content"],
                "created_at": message["created_at"],
                "conversation_id": message["conversation_id"]
            })
            
        # Count query to get total (note: this is expensive in Cassandra)
        # In a production app, you might use approximate counts or other strategies
        count_query = """
            SELECT COUNT(*) as total
            FROM messages
            WHERE conversation_id = %s
        """
        count_result = cassandra_client.execute(count_query, (conversation_id,))
        total = count_result[0]["total"] if count_result else 0
        
        return {
            "total": total,
            "page": page,
            "limit": limit,
            "data": formatted_messages
        }
    
    @staticmethod
    async def get_messages_before_timestamp(
        conversation_id: int,
        before_timestamp: datetime,
        page: int = 1,
        limit: int = 20
    ) -> Dict[str, Any]:
        """
        Get messages before a timestamp with pagination.
        
        Args:
            conversation_id: ID of the conversation
            before_timestamp: Get messages before this timestamp
            page: Page number (1-indexed)
            limit: Maximum number of messages per page
            
        Returns:
            Dictionary with paginated results
        """
        # Calculate pagination offset
        offset = (page - 1) * limit
        
        # Query for messages before timestamp
        query = """
            SELECT message_id, sender_id, receiver_id, content, created_at, conversation_id 
            FROM messages 
            WHERE conversation_id = %s AND created_at < %s
            LIMIT %s
        """
        
        result = cassandra_client.execute(query, 
            (conversation_id, before_timestamp, limit + offset)
        )
        
        # Apply offset and limit
        messages = result[offset:offset+limit]
        
        # Format messages to match our schema (with string IDs from UUIDs)
        formatted_messages = []
        for message in messages:
            formatted_messages.append({
                "id": str(message["message_id"]),  # Convert UUID to string
                "sender_id": message["sender_id"],
                "receiver_id": message["receiver_id"],
                "content": message["content"],
                "created_at": message["created_at"],
                "conversation_id": message["conversation_id"]
            })
            
        # Count query (with timestamp filter)
        count_query = """
            SELECT COUNT(*) as total
            FROM messages
            WHERE conversation_id = %s AND created_at < %s
        """
        count_result = cassandra_client.execute(count_query, 
            (conversation_id, before_timestamp)
        )
        total = count_result[0]["total"] if count_result else 0
        
        return {
            "total": total,
            "page": page,
            "limit": limit,
            "data": formatted_messages
        }


class ConversationModel:
    """
    Conversation model for interacting with the conversations-related tables.
    """
    
    @staticmethod
    async def get_user_conversations(
        user_id: int,
        page: int = 1,
        limit: int = 20
    ) -> Dict[str, Any]:
        """
        Get conversations for a user with pagination.
        
        Args:
            user_id: ID of the user
            page: Page number (1-indexed)
            limit: Maximum number of conversations per page
            
        Returns:
            Dictionary with paginated conversations
        """
        # Calculate pagination offset
        offset = (page - 1) * limit
        
        # Query for conversations
        query = """
            SELECT conversation_id, other_user_id, last_message_at, last_message_content
            FROM conversations_by_user
            WHERE user_id = %s
            LIMIT %s
        """
        
        result = cassandra_client.execute(query, (user_id, limit + offset))
        
        # Apply offset and limit
        conversations_raw = result[offset:offset+limit]
        
        # Get detailed conversation information
        conversations = []
        for conv_raw in conversations_raw:
            # Get the full conversation details
            conv_details = await ConversationModel.get_conversation(conv_raw["conversation_id"])
            if conv_details:
                # Add last message details
                conv_details["last_message_at"] = conv_raw["last_message_at"]
                conv_details["last_message_content"] = conv_raw["last_message_content"]
                conversations.append(conv_details)
        
        # Count query
        count_query = """
            SELECT COUNT(*) as total
            FROM conversations_by_user
            WHERE user_id = %s
        """
        count_result = cassandra_client.execute(count_query, (user_id,))
        total = count_result[0]["total"] if count_result else 0
        
        return {
            "total": total,
            "page": page,
            "limit": limit,
            "data": conversations
        }
    
    @staticmethod
    async def get_conversation(conversation_id: int) -> Optional[Dict[str, Any]]:
        """
        Get a conversation by ID.
        
        Args:
            conversation_id: ID of the conversation
            
        Returns:
            Dictionary with conversation details or None if not found
        """
        # Get conversation details, including last message info in a single query
        query = """
            SELECT conversation_id, user1_id, user2_id, created_at, last_message_at, last_message_content
            FROM conversations
            WHERE conversation_id = %s
        """
        
        result = cassandra_client.execute(query, (conversation_id,))
        
        if not result:
            return None
        
        conversation = result[0]
        print(conversation["last_message_content"])
        
        # Return conversation with last message details
        return {
            "id": conversation["conversation_id"],
            "user1_id": conversation["user1_id"],
            "user2_id": conversation["user2_id"],
            "created_at": conversation["created_at"],
            "last_message_at": conversation["last_message_at"],
            "last_message_content": conversation["last_message_content"]
        }
    
    @staticmethod
    async def create_or_get_conversation(user1_id: int, user2_id: int) -> Dict[str, Any]:
        """
        Get an existing conversation between two users or create a new one.
        
        This ensures we don't create duplicate conversations between the same users.
        
        Args:
            user1_id: ID of the first user
            user2_id: ID of the second user
            
        Returns:
            Dictionary with conversation details
        """
        # Sort user IDs to maintain consistency
        lower_id = min(user1_id, user2_id)
        higher_id = max(user1_id, user2_id)
        
        # Check if conversation already exists
        # We need to use ALLOW FILTERING here since we're querying by user IDs
        # In a production system, you might use a secondary index or a materialized view
        query = """
            SELECT conversation_id, user1_id, user2_id, created_at, last_message_at, last_message_content
            FROM conversations
            WHERE user1_id = %s AND user2_id = %s
            ALLOW FILTERING
        """
        
        result = cassandra_client.execute(query, (lower_id, higher_id))
        
        if result:
            # Return existing conversation
            conversation = result[0]
            return {
                "id": conversation["conversation_id"],
                "user1_id": conversation["user1_id"],
                "user2_id": conversation["user2_id"],
                "created_at": conversation["created_at"],
                "last_message_at": conversation["last_message_at"],
                "last_message_content": conversation["last_message_content"]
            }
        else:
            # Create new conversation
            now = datetime.now()
            
            # For simplicity, we'll generate a conversation ID from user IDs and timestamp
            # This is just for demonstration; in production use a better ID strategy
            conversation_id = abs(hash(f"{lower_id}:{higher_id}:{now.timestamp()}") % 100000000)
                
            # Insert new conversation with empty last message fields
            insert_query = """
                INSERT INTO conversations
                (conversation_id, user1_id, user2_id, created_at, last_message_at, last_message_content)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            cassandra_client.execute(insert_query, 
                (conversation_id, lower_id, higher_id, now, None, None)
            )
            
            # Return the newly created conversation
            return {
                "id": conversation_id,
                "user1_id": lower_id,
                "user2_id": higher_id,
                "created_at": now,
                "last_message_at": None,
                "last_message_content": None
            }