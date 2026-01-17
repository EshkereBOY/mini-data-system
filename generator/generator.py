import os
import time
import random
import psycopg2
from datetime import datetime

# Настройки подключения из переменных окружения
DB_NAME = os.getenv("POSTGRES_DB", "shop_db")
DB_USER = os.getenv("POSTGRES_USER", "admin")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "password")
DB_HOST = os.getenv("DB_HOST", "db")

CATEGORIES = {
    "Электроника": ["Смартфон", "Ноутбук", "Наушники", "Часы"],
    "Дом": ["Лампа", "Пылесос", "Кофеварка", "Тостер"],
    "Спорт": ["Гантели", "Коврик для йоги", "Мяч", "Скакалка"]
}
CITIES = ["Москва", "Санкт-Петербург", "Новосибирск", "Екатеринбург", "Казань"]

def connect_db():
    while True:
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST
            )
            return conn
        except Exception:
            print("Ожидание готовности базы данных...")
            time.sleep(2)

def run_generator():
    conn = connect_db()
    cur = conn.cursor()
    
    # СОЗДАЕМ ТАБЛИЦУ ЗДЕСЬ, а не через init.sql
    cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id SERIAL PRIMARY KEY,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            product TEXT NOT NULL,
            category TEXT NOT NULL,
            price DECIMAL(10, 2) NOT NULL,
            quantity INTEGER NOT NULL,
            city TEXT NOT NULL
        );
    """)
    conn.commit()
    cur.close()
    
    print("Таблица проверена. Начинаю генерацию...")
    
    while True:
        try:
            cur = conn.cursor()
            category = random.choice(list(CATEGORIES.keys()))
            product = random.choice(CATEGORIES[category])
            price = round(random.uniform(500, 75000), 2)
            quantity = random.randint(1, 3)
            city = random.choice(CITIES)

            query = """
                INSERT INTO orders (product, category, price, quantity, city)
                VALUES (%s, %s, %s, %s, %s)
            """
            cur.execute(query, (product, category, price, quantity, city))
            conn.commit()
            cur.close()
            
            print(f"Заказ создан: {product} ({city})")
            time.sleep(1) # Периодичность — 1 раз в секунду
        except Exception as e:
            print(f"Ошибка: {e}. Переподключение...")
            conn = connect_db()

if __name__ == "__main__":
    run_generator()