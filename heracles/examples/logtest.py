import logging

class InfoToPrintHandler(logging.Handler):
    def emit(self, record):
        if record.levelno == logging.INFO:
            # Use the normal print instead of the logger's formatting
            print(record.getMessage())

# Example setup
logger = logging.getLogger("mylogger")
logger.setLevel(logging.DEBUG)  # Allow all levels

# Attach our custom handler
logger.addHandler(InfoToPrintHandler())

# Add a normal handler for other levels (optional)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.WARNING)
logger.addHandler(console_handler)

# Test messages
logger.debug("This is DEBUG (won't show).")
logger.info("This is INFO (will be printed with print).")
logger.warning("This is WARNING (will go through normal handler).")
