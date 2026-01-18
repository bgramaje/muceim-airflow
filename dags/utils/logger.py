"""
Logging utilities for Airflow DAGs.

This module provides a standardized logging interface that integrates
seamlessly with Airflow's logging system. All logs will appear in the
Airflow UI under the task logs.

Usage:
    from utils.logger import get_logger
    
    logger = get_logger(__name__)
    logger.info("Processing started")
    logger.warning("Low memory detected")
    logger.error("Failed to connect")
"""

import logging
import sys
from typing import Optional


def get_logger(name: Optional[str] = None, context: Optional[dict] = None) -> logging.Logger:
    """
    Get a logger instance configured for Airflow.
    
    In Airflow tasks, the logger automatically includes task context.
    Outside of tasks, it behaves like a standard Python logger.
    
    Args:
        name: Logger name (typically __name__). If None, uses root logger.
        context: Optional Airflow context dict. If provided, adds context info to logs.
    
    Returns:
        Configured logger instance
    
    Example:
        # In an Airflow task
        @task
        def my_task(**context):
            logger = get_logger(__name__, context)
            logger.info("Task started")
        
        # In a utility function
        def my_function():
            logger = get_logger(__name__)
            logger.info("Function called")
    """
    # Try to get Airflow task logger if available
    try:
        from airflow.operators.python import get_current_context
        from airflow.utils.log.logging_mixin import LoggingMixin
        
        # If we're in an Airflow task context, use Airflow's logger
        try:
            task_context = get_current_context()
            if task_context:
                # Use Airflow's task logger which includes task_id, dag_id, etc.
                logger = logging.getLogger(f"airflow.task.{task_context.get('task', {}).get('task_id', 'unknown')}")
                return logger
        except Exception:
            pass  # Fall back to standard logger
    except ImportError:
        pass  # Airflow not available, use standard logger
    
    # Standard Python logger
    if name:
        logger = logging.getLogger(name)
    else:
        logger = logging.getLogger()
    
    # Configure logger if not already configured
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    
    # Add context information if provided
    if context:
        # Add context adapter to include DAG/task info in logs
        class ContextAdapter(logging.LoggerAdapter):
            def process(self, msg, kwargs):
                dag_id = self.extra.get('dag_id', 'unknown')
                task_id = self.extra.get('task_id', 'unknown')
                return f"[{dag_id}.{task_id}] {msg}", kwargs
        
        # Extract DAG ID - handle both dict and DAG object
        dag_obj = context.get('dag')
        if dag_obj:
            if hasattr(dag_obj, 'dag_id'):
                dag_id = dag_obj.dag_id
            elif isinstance(dag_obj, dict):
                dag_id = dag_obj.get('dag_id', 'unknown')
            else:
                dag_id = 'unknown'
        else:
            dag_id = 'unknown'
        
        # Extract Task ID - handle both dict and TaskInstance object
        task_instance = context.get('task_instance')
        if task_instance:
            if hasattr(task_instance, 'task_id'):
                task_id = task_instance.task_id
            elif isinstance(task_instance, dict):
                task_id = task_instance.get('task_id', 'unknown')
            else:
                task_id = 'unknown'
        else:
            task_id = 'unknown'
        
        context_info = {
            'dag_id': dag_id,
            'task_id': task_id,
        }
        logger = ContextAdapter(logger, context_info)
    
    return logger


def setup_logging(level: int = logging.INFO):
    """
    Configure root logging for the application.
    
    Args:
        level: Logging level (logging.DEBUG, logging.INFO, etc.)
    """
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[logging.StreamHandler(sys.stdout)]
    )
