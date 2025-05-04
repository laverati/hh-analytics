import requests
import pandas as pd
from datetime import datetime, timedelta
import time

# === Настройки ===
role_ids = [10, 12, 56, 57, 153, 155]  # Роли аналитиков
region_ids = [1, 2, 3, 66, 88, 54, 68, 76, 111, 104]  # ID городов: Москва, Питер, Новосибирск и др.
date_from = (datetime(2025, 5, 3) - timedelta(days=30)).strftime('%Y-%m-%d')
per_page = 100
max_pages = 20
all_vacancies = []

print(f"📦 Сбор вакансий аналитиков по регионам с {date_from} по 03.05.2025...\n")

# === Сбор данных ===
for region_id in region_ids:
    print(f"🌍 Регион ID: {region_id}")
    for role_id in role_ids:
        print(f"  🔍 Роль ID: {role_id}")
        for page in range(max_pages):
            params = {
                'professional_role': role_id,
                'date_from': date_from,
                'page': page,
                'per_page': per_page,
                'text': 'аналитик',
                'area': region_id
            }

            try:
                print(f"    → Страница {page}...", end=" ")
                response = requests.get("https://api.hh.ru/vacancies", params=params, timeout=10)
                print(f"Статус: {response.status_code}")

                if response.status_code != 200:
                    print("    ❌ Ошибка, пропуск страницы")
                    break

                data = response.json()
                items = data.get("items", [])
                print(f"    ✅ Получено: {len(items)} вакансий")

                all_vacancies.extend(items)

                if page >= data.get("pages", 0) - 1:
                    break

                time.sleep(0.5)

            except Exception as e:
                print(f"    ❌ Ошибка запроса: {e}")
                break

# === Обработка данных ===
safe_data = []
for item in all_vacancies:
    salary = item.get("salary") or {}
    vac_id = item.get("id")
    safe_data.append({
        "name": item.get("name"),
        "salary_from": salary.get("from"),
        "salary_to": salary.get("to"),
        "employment": item.get("employment", {}).get("name"),
        "schedule": item.get("schedule", {}).get("name"),  # 👈 Формат работы
        "experience": item.get("experience", {}).get("name"),
        "area": item.get("area", {}).get("name"),
        "published_at": item.get("published_at"),
        "url": f"https://hh.ru/vacancy/{vac_id}" if vac_id else None
    })

df = pd.DataFrame(safe_data)

# Приведение типов
df["salary_from"] = pd.to_numeric(df["salary_from"], errors="coerce")
df["salary_to"] = pd.to_numeric(df["salary_to"], errors="coerce")
df["published_at"] = pd.to_datetime(df["published_at"], errors="coerce")

# Сохранение
df.to_csv("analytics.csv", index=False, encoding="utf-8-sig")
print(f"\n✅ Сохранено {len(df)} вакансий в 'analytics.csv'")
