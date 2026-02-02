import logging
import sys
import datetime

class CustomFormatter(logging.Formatter):
    def format(self, record):
        # Format: Mon, 02 Jan 2006 15:04:05 [INFO]: message ...
        # Python datetime format: %a, %d %b %Y %H:%M:%S
        timestamp = datetime.datetime.fromtimestamp(record.created).strftime('%a, %d %b %Y %H:%M:%S')
        level = record.levelname
        msg = record.getMessage()
        
        # Handle extra attributes if they exist (simulating structured logging)
        extras = []
        if hasattr(record, 'args') and isinstance(record.args, dict):
             # If logger.info("msg", {"key": "val"}) is used, args might be the dict?
             # Standard python logging: logger.info("msg", extra={"key": "val"}) merges into record.
             # We want to support explicit kwargs style or dict passing like the other langs if possible.
             # But here we probably stick to simply formatting the message or extracting 'extra' fields if present in record.__dict__
             pass

        # For simplicity and matching the others:
        # We will assume usage: logger.info("Message", extra={'key': 'value'})
        # And we iterate typical record attributes to find extras? 
        # Easier: The other implementations pass a meta object. 
        # We can implement a wrapper or just use standard logging and format `extra` param.
        
        # Let's inspect record.__dict__ for extras that are not standard.
        standard_attrs = {
            'name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 'filename', 'module', 'exc_info',
            'exc_text', 'stack_info', 'lineno', 'funcName', 'created', 'msecs', 'relativeCreated', 
            'thread', 'threadName', 'processName', 'process', 'message'
        }
        
        meta_parts = []
        for key, value in record.__dict__.items():
            if key not in standard_attrs:
                 meta_parts.append(f"{key}={value}")
        
        meta_str = ""
        if meta_parts:
            meta_str = " | " + " ".join(meta_parts)

        return f"{timestamp} [{level}]: {msg}{meta_str}"

def init_logger():
    logger = logging.getLogger("YugabyteDB")
    logger.setLevel(logging.INFO)
    
    # Check if handler already exists to avoid dupes
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(CustomFormatter())
        logger.addHandler(handler)
    
    return logger

logger = init_logger()
