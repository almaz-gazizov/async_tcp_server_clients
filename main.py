import asyncio
import logging
import os
import random
import sqlite3
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def init_db():
    '''Инициализация базы данных SQLite.'''
    db_path = os.path.join(os.path.dirname(__file__), 'database.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute('''CREATE TABLE IF NOT EXISTS requests
                      (id INTEGER PRIMARY KEY AUTOINCREMENT,
                       message TEXT NOT NULL,
                       timestamp TEXT NOT NULL)''')
    conn.commit()
    conn.close()


def store_request(message):
    '''Функция для записи данных в базу данных.'''
    db_path = os.path.join(os.path.dirname(__file__), 'database.db')
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    cursor.execute(
        "INSERT INTO requests (message, timestamp) VALUES (?, ?)",
        (message, timestamp)
    )
    conn.commit()
    conn.close()


async def handle_client(reader, writer):
    '''Асинхронная функция для обработки клиентского соединения.'''
    addr = writer.get_extra_info('peername')
    logger.info(f"Установлено соединение с {addr}")

    try:
        while True:
            data = await reader.read(100)
            if not data:
                break
            message = data.decode()
            logger.info(f"Получено сообщение: {message} от {addr}")

            # Записываем запрос в базу данных
            store_request(message)

            logger.info(f"Отправка эхо-ответа: {message} клиенту {addr}")
            writer.write(data)
            await writer.drain()

    except asyncio.CancelledError:
        logger.info(f"Соединение с {addr} закрыто")
    finally:
        writer.close()
        await writer.wait_closed()


async def run_server(host='127.0.0.1', port=8800):
    '''Асинхронная функция для запуска TCP-сервера.'''
    server = await asyncio.start_server(handle_client, host, port)
    addr = server.sockets[0].getsockname()
    logger.info(f'Сервер запущен и слушает на {addr}')

    async with server:
        await server.serve_forever()


async def tcp_client(host='127.0.0.1', port=8800):
    '''Асинхронная функция для работы TCP-клиента.'''
    await asyncio.sleep(2)
    reader, writer = await asyncio.open_connection(host, port)

    for i in range(5):
        await asyncio.sleep(random.randint(5, 10))
        message = f'Сообщение {i+1} от клиента {id(writer)}'
        logger.info(f'Отправка: {message}')
        writer.write(message.encode())
        await writer.drain()

        data = await reader.read(100)
        logger.info(f'Получен эхо-ответ: {data.decode()}')

    logger.info(f'Закрытие соединения от клиента {id(writer)}')
    writer.close()
    await writer.wait_closed()


async def main():
    '''Основная функция, объединяющая сервер и клиентов.'''
    init_db()
    server_task = asyncio.create_task(run_server())
    clients = [asyncio.create_task(tcp_client()) for _ in range(10)]
    await asyncio.gather(*clients)
    server_task.cancel()
    try:
        await server_task
    except asyncio.CancelledError:
        logger.info("Сервер был завершён.")


if __name__ == '__main__':
    asyncio.run(main())
