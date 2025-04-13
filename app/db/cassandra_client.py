"""
Cassandra client for the Messenger application.
This provides a connection to the Cassandra database.
"""
import os
import uuid
from typing import List, Dict, Any, Optional, Union, Tuple
from datetime import datetime
import logging

from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement, dict_factory

logger = logging.getLogger(__name__)

class CassandraClient:
    """Singleton Cassandra client for the application."""
    
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(CassandraClient, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        """Initialize the Cassandra connection."""
        if self._initialized:
            return
        
        self.host = os.getenv("CASSANDRA_HOST", "localhost")
        self.port = int(os.getenv("CASSANDRA_PORT", "9042"))
        self.keyspace = os.getenv("CASSANDRA_KEYSPACE", "messenger")
        
        self.cluster = None
        self.session = None
        self.connect()
        
        self._initialized = True
    
    def connect(self) -> None:
        """Connect to the Cassandra cluster."""
        try:
            self.cluster = Cluster([self.host])
            self.session = self.cluster.connect(self.keyspace)
            self.session.row_factory = dict_factory
            logger.info(f"Connected to Cassandra at {self.host}:{self.port}, keyspace: {self.keyspace}")
        except Exception as e:
            logger.error(f"Failed to connect to Cassandra: {str(e)}")
            raise
    
    def close(self) -> None:
        """Close the Cassandra connection."""
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Cassandra connection closed")
    
    def execute(self, query: str, params: Union[Tuple, Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a CQL query.
        
        Args:
            query: The CQL query string
            params: The parameters for the query as either a tuple or dictionary
            
        Returns:
            List of rows as dictionaries
        """
        if not self.session:
            self.connect()
        
        try:
            statement = SimpleStatement(query)
            
            # Ensure params is always passed in the correct format
            if params is None:
                result = self.session.execute(statement)
            else:
                result = self.session.execute(statement, params)
                
            return list(result)
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
    
    def execute_async(self, query: str, params: Union[Tuple, Dict[str, Any]] = None):
        """
        Execute a CQL query asynchronously.
        
        Args:
            query: The CQL query string
            params: The parameters for the query as either a tuple or dictionary
            
        Returns:
            Async result object
        """
        if not self.session:
            self.connect()
        
        try:
            statement = SimpleStatement(query)
            
            # Ensure params is always passed in the correct format
            if params is None:
                return self.session.execute_async(statement)
            else:
                return self.session.execute_async(statement, params)
        except Exception as e:
            logger.error(f"Async query execution failed: {str(e)}")
            raise
    
    def get_session(self) -> Session:
        """Get the Cassandra session."""
        if not self.session:
            self.connect()
        return self.session

# Create a global instance
cassandra_client = CassandraClient()