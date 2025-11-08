import pandas as pd
from tqdm import tqdm

input_file = "xaba.csv"
output_file = "contacts_with_emails222.csv"

# Размер порции (сколько строк читаем за раз)
chunksize = 50000

# Пробуем определить количество строк (если файл гигантский — можно пропустить)
try:
    with open(input_file, 'r', encoding='utf-8', errors='ignore') as f:
        total_lines = sum(1 for _ in f) - 1  # -1 потому что заголовок
except Exception as e:
    print("⚠️ Не удалось подсчитать строки:", e)
    total_lines = None

rows_with_emails = []

# читаем CSV по частям
for chunk in tqdm(
    pd.read_csv(input_file, chunksize=chunksize, encoding='utf-8', on_bad_lines='skip'),
    total=(total_lines // chunksize + 1) if total_lines else None,
    desc="Обработка CSV"
):
    # фильтруем по e-mail
    mask = chunk["email"].astype(str).str.contains("@", na=False)
    if "email2" in chunk.columns:
        mask |= chunk["email2"].astype(str).str.contains("@", na=False)
    rows_with_emails.append(chunk[mask])

# объединяем результат
if rows_with_emails:
    filtered = pd.concat(rows_with_emails, ignore_index=True)
    filtered.to_csv(output_file, index=False)
    print(f"✅ Готово! Сохранено {len(filtered)} строк с e-mail в {output_file}")
else:
    print("❌ Не найдено строк с e-mail.")
1