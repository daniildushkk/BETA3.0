async def migrate_db():
    """Миграция базы данных для добавления поля language"""
    try:
        async with aiosqlite.connect('events.db') as db:
            # Проверяем, есть ли столбец language
            cursor = await db.execute("PRAGMA table_info(events)")
            columns = await cursor.fetchall()
            column_names = [column[1] for column in columns]

            if 'language' not in column_names:
                logger.info("🔄 Добавляем поле language в таблицу events...")
                await db.execute('ALTER TABLE events ADD COLUMN language TEXT NOT NULL DEFAULT "ru"')
                await db.commit()
                logger.info("✅ Миграция базы данных завершена")
            else:
                logger.info("✅ База данных уже актуальна")

    except Exception as e:
        logger.error(f"❌ Ошибка миграции БД: {e}")

# Обновляем функцию main():
async def main():
    try:
        await init_db()
        await migrate_db()  # <-- Добавляем миграцию
        logger.info("✅ База данных инициализирована")

        # Автоматический парсинг при старте (в фоне)
        asyncio.create_task(auto_parse_events())

        logger.info("🚀 Запуск бота с мультиязычной поддержкой...")
        await safe_start_polling()

    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        await asyncio.sleep(2)