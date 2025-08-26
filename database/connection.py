# # MongoDB connection management

# def get_mongodb_client():
#     """Create and return MongoDB client"""

# def get_database():
#     """Get specific database instance"""

# def test_connection():
#     """Test MongoDB connection"""

# def close_connection():
#     """Close MongoDB connection"""

# database/connection.py
"""
MongoDB connection management module.

Provides a singleton client for reuse across the app,
plus helper functions to get a database instance,
test connectivity, and close the connection cleanly.
"""

from pymongo import MongoClient
from config.settings import get_database_config
from utils.logger import get_logger, log_error

# Global singleton MongoDB client
_mongo_client: MongoClient | None = None


def get_mongodb_client() -> MongoClient:
    """
    Return a singleton MongoDB client instance.
    Creates one if it does not already exist.
    """
    global _mongo_client
    if _mongo_client is None:
        db_config = get_database_config()
        logger = get_logger("database")

        try:
            _mongo_client = MongoClient(db_config["connection_string"])
            logger.info(
                "MongoDB client created",
                connection=db_config["connection_string"],
            )
        except Exception as e:
            log_error(e, {"operation": "mongo_client_init", "config": db_config})
            raise  # fail fast if client cannot be created
    return _mongo_client


def get_database():
    """
    Return the configured MongoDB database instance.
    """
    client = get_mongodb_client()
    db_config = get_database_config()
    return client[db_config["database_name"]]


def test_connection() -> bool:
    """
    Ping MongoDB to test connectivity.
    Logs errors instead of crashing.
    Returns:
        bool: True if connection succeeds, False otherwise.
    """
    logger = get_logger("database")
    try:
        client = get_mongodb_client()
        client.admin.command("ping")
        logger.info("MongoDB connection successful", database=get_database().name)
        return True
    except Exception as e:
        log_error(e, {"operation": "mongo_test_connection"})
        return False


def close_connection():
    """
    Close MongoDB connection explicitly.
    Not strictly required (driver handles this on exit),
    but good practice for clean shutdowns.
    """
    global _mongo_client
    logger = get_logger("database")

    if _mongo_client is not None:
        try:
            _mongo_client.close()
            logger.info("MongoDB connection closed")
        except Exception as e:
            log_error(e, {"operation": "mongo_close"})
        finally:
            _mongo_client = None
