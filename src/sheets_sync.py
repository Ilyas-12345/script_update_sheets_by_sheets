from datetime import datetime

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
            with open(self.processed_file, 'w', encoding='utf-8') as f:
                json.dump(list(self.processed_rows), f, ensure_ascii=False, indent=2)
        except Exception as e:
            print(f"Ошибка при сохранении processed_rows: {e}")

    def create_row_signature(self, row):
        """Создать уникальную сигнатуру для строки"""
        key_data = str(row[1])
        return key_data

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

                        new_row = [row[1], row[2], row[9], row[3], row[7], row[11], row[12], row[13], row[5]]
                        if row[5].replace('.', '').isdigit():
                            try:
                                date_num = float(row[5])
                                if 10000 < date_num < 50000:  # Диапазон дат
                                    # Преобразуем в datetime и затем в строку нужного формата
                                    converted_date = self.convert_serial_date_to_string(date_num)
                                    new_row[8] = converted_date
                                    print(f"Преобразована дата: {row[5]} -> {converted_date}")
                            except ValueError:
                                pass  # Не число, оставляем как есть

                        self.insert_row_from_column_a(target_sheet, new_row)

                        self.processed_rows.add(row_signature)
                        new_rows_copied += 1
                        time.sleep(1.1)

                        print(f"Строка {row_num} скопирована в лист '{self.target_sheet_name}'")

            if new_rows_copied > 0:
                self.save_processed_rows()
                print(f"Скопировано новых строк: {new_rows_copied}")
            else:
                print("Новых строк для копирования не найдено")

        except Exception as e:
            print(f"Ошибка при проверке строк: {e}")

    def convert_serial_date_to_string(self, serial_date):
        """Преобразует серийный номер даты в строку формата DD.MM.YYYY"""
        try:
            from datetime import datetime, timedelta
            # Google Sheets считает от 30 декабря 1899 года
            base_date = datetime(1899, 12, 30)
            target_date = base_date + timedelta(days=int(serial_date))
            return target_date.strftime("%d.%m.%Y")
        except Exception as e:
            print(f"Ошибка преобразования даты {serial_date}: {e}")
            return str(serial_date)

    def delete_row_from_file(self):
        """
        Удаляет строку из файла, содержащую указанное значение
        """
        try:
            if not os.path.exists(self.processed_file):
                with open(self.processed_file, 'w', encoding='utf-8') as file:
                    json.dump([], file, ensure_ascii=False, indent=2)
                print(f"Создан новый файл: {self.processed_file}")
                return

            if os.path.getsize(self.processed_file) == 0:
                print("Файл пустой")
                return

            with open(self.processed_file, 'r', encoding='utf-8') as file:
                lines = json.load(file)

            # Ищем строку, содержащую search_value, и фильтруем её
            spreadsheet = self.get_spreadsheet()
            source_sheet = spreadsheet.worksheet(self.source_sheet_name)
            all_data = source_sheet.get_all_values()
            data_table = [row[1].strip() for row in all_data if row]

            new_lines = []
            for line in lines:
                clean_line = line.strip()

                if clean_line in data_table:
                    new_lines.append(line)
                else:
                    pass
                    print(f"Удалена строка: {clean_line}")

            # Записываем обновленные данные обратно в файл
            with open(self.processed_file, 'w', encoding='utf-8') as file:
                json.dump(new_lines, file, ensure_ascii=False, indent=2)

        except Exception as e:
            print(f"Ошибка при удалении строки из файла: {e}")

    def insert_row_from_column_a(self, worksheet, row_data):
        """Вставить строку начиная с колонки A"""
        try:
            all_data = worksheet.get_all_values()
            empty_row_num = len(all_data) + 1
            range_start = f"A{empty_row_num}"

            # Используем USER_ENTERED с уже преобразованными данными
            worksheet.update([row_data], range_start, value_input_option='USER_ENTERED')

            print(f"Вставлена строка: {row_data}")

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
                print(f"Запуск - время {datetime.now().strftime('%H:%M:%S')}")
                self.delete_row_from_file()
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