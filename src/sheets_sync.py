import gspread
import time
import json
import os

from dotenv import load_dotenv
from google.oauth2.service_account import Credentials

load_dotenv()

class GoogleSheetsSync:
    def __init__(self):
        self.spreadsheet_id = os.getenv('SPREADSHEET_ID',)
        self.source_sheet_name = os.getenv('SOURCE_SHEET_NAME')
        self.target_sheet_name = os.getenv('TARGET_SHEET_NAME')
        self.check_interval = int(os.getenv('CHECK_INTERVAL'))
        self.BASE_DIR = os.path.dirname(os.path.abspath(__file__))

        self.client = self.setup_google_sheets()
        self.processed_file = "./processed_rows.json"
        self.processed_rows = self.load_processed_rows()

    def setup_google_sheets(self):
        """Настройка подключения к Google Sheets"""
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        SERVICE_ACCOUNT_FILE = "./service_acc.json"

        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            raise FileNotFoundError(f"Файл учетных данных не найден: {SERVICE_ACCOUNT_FILE}")

        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        client = gspread.authorize(creds)
        return client

    def get_spreadsheet(self):
        """Получить доступ к таблице"""
        return self.client.open_by_key(self.spreadsheet_id)

    def load_processed_rows(self):
        """Загрузить список обработанных строк из файла"""
        try:
            if os.path.exists(self.processed_file):
                with open(self.processed_file, 'r', encoding='utf-8') as f:
                    return set(json.load(f))
            return set()
        except Exception as e:
            print(f"Ошибка при загрузке processed_rows: {e}")
            return set()

    def save_processed_rows(self):
        """Сохранить список обработанных строк в файл"""
        try:
            # Создаем директорию если не существует
            os.makedirs(os.path.dirname(self.processed_file), exist_ok=True)
            with open(self.processed_file, 'w', encoding='utf-8') as f:
                json.dump(list(self.processed_rows), f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Ошибка при сохранении processed_rows: {e}")

    def create_row_signature(self, row):
        """Создать уникальную сигнатуру для строки"""
        key_data = [str(row[i]) for i in [0, 1, 2, 3]]
        return "|".join(key_data)

    def check_and_copy_rows(self):
        """Проверить и скопировать строки с 'одобрена' в колонке E"""
        try:
            spreadsheet = self.get_spreadsheet()
            source_sheet = spreadsheet.worksheet(self.source_sheet_name)
            target_sheet = spreadsheet.worksheet(self.target_sheet_name)

            all_data = source_sheet.get_all_values()
            new_rows_copied = 0

            for row_num, row in enumerate(all_data, 1):
                if not any(row):
                    continue

                if len(row) > 4 and row[4].strip().lower() == "одобрена":
                    row_signature = self.create_row_signature(row)

                    if row_signature not in self.processed_rows:
                        print(f"Найдена новая строка для копирования: строка {row_num}")

                        new_row = [row[1], row[2], row[0], row[3], row[7], row[11], row[12], row[13], row[5]]
                        self.insert_row_from_column_a(target_sheet, new_row)

                        self.processed_rows.add(row_signature)
                        new_rows_copied += 1

                        print(f"Строка {row_num} скопирована в лист '{self.target_sheet_name}'")

            if new_rows_copied > 0:
                self.save_processed_rows()
                print(f"Скопировано новых строк: {new_rows_copied}")
            else:
                print("Новых строк для копирования не найдено")

        except Exception as e:
            print(f"Ошибка при проверке строк: {e}")

    def insert_row_from_column_a(self, worksheet, row_data):
        """Вставить строку начиная с колонки A"""
        try:
            # Находим первую полностью пустую строку
            all_data = worksheet.get_all_values()
            empty_row_num = len(all_data) + 1

            # Формируем диапазон для вставки (начинается с колонки A)
            range_start = f"A{empty_row_num}"

            # Вставляем данные начиная с колонки A
            worksheet.update([row_data], range_start)

        except Exception as e:
            print(f"Ошибка при вставке строки: {e}")

    def monitor_changes(self):
        """Постоянный мониторинг изменений"""
        print("=" * 50)
        print("Google Sheets Sync Service запущен")
        print(f"Таблица: {self.spreadsheet_id}")
        print(f"Источник: {self.source_sheet_name}")
        print(f"Цель: {self.target_sheet_name}")
        print(f"Интервал проверки: {self.check_interval} секунд")
        print(f"Обработанных строк: {len(self.processed_rows)}")
        print("=" * 50)
        print("Для остановки нажмите Ctrl+C")

        while True:
            try:
                self.check_and_copy_rows()
                time.sleep(self.check_interval)
            except KeyboardInterrupt:
                print("Сервис остановлен")
                break
            except Exception as e:
                print(f"Ошибка в цикле мониторинга: {e}")
                time.sleep(30)


if __name__ == "__main__":
    sync = GoogleSheetsSync()
    sync.monitor_changes()