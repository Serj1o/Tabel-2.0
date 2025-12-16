import asyncio
import logging
from datetime import datetime


class DatabaseLogHandler(logging.Handler):
    """Логгер, сохраняющий записи в таблицу log_entries."""

    def __init__(self, pool):
        super().__init__()
        self.pool = pool

    async def _write(self, record: logging.LogRecord):
        message = self.format(record)
        created_at = datetime.fromtimestamp(record.created)

        async with self.pool.acquire() as conn:
            await conn.execute(
                '''
                INSERT INTO log_entries (created_at, logger, level, message)
                VALUES ($1, $2, $3, $4)
                ''',
                created_at,
                record.name,
                record.levelname,
                message,
            )

    def emit(self, record: logging.LogRecord):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        loop.create_task(self._write(record))
