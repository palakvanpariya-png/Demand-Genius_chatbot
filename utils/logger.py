# # Logging configuration

# def setup_logging():
#     """Setup application logging configuration"""

# def get_logger(name: str):
#     """Get logger instance for specific module"""

# def log_query_processing(tenant_id: str, query: str, response_type: str):
#     """Log query processing details"""

# def log_controlflow_execution(task_name: str, execution_time: float):
#     """Log ControlFlow task execution metrics"""

# def log_error(error: Exception, context: dict):
#     """Log errors with context information"""

import logging
import structlog
import os


def setup_logging():
    """Setup application logging configuration"""
    log_file = os.path.join("logs", "app.log")

    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )

    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.KeyValueRenderer(key_order=["event", "tenant_id"]),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str):
    """Get logger instance for specific module"""
    return structlog.get_logger(name)


def log_query_processing(tenant_id: str, query: str, response_type: str):
    """Log query processing details"""
    logger = get_logger("query")
    logger.info(
        "Processed query",
        tenant_id=tenant_id,
        query=query,
        response_type=response_type,
    )


def log_controlflow_execution(task_name: str, execution_time: float):
    """Log ControlFlow task execution metrics"""
    logger = get_logger("controlflow")
    logger.info(
        "Executed ControlFlow task",
        task=task_name,
        execution_time=f"{execution_time:.2f}s",
    )


def log_error(error: Exception, context: dict):
    """Log errors with context information"""
    logger = get_logger("error")
    logger.error(
        "Error occurred",
        error_message=str(error),
        context=context,
    )
