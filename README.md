# Подготовка к уведомлению

## Содержание

- [Установка](#установка)
- [Использование](#использование)

## Установка


1. Клонируйте репозиторий:
   
   git clone https://github.com/TheVitRub/uvedomlenia.git   

2. Установите зависимости:

    pip install -r requirements.txt

3. Заполните .env
HOST=
PASS=
LOGIN=
PORT=
DATABASE_NAME=
path_sales = ссылка на таблицу

## Использование

Запускаем main. В файле есть 
    sort_pipl.start_first_krug()
    sort_pipl.start_second_krug()
Первая команда запускает проверку по первому кругу, вторая по второму.
По итогу мы должны получить 10 файлов типа Itog(номер аудитории).xlsx и Itog(номер аудитории)_second.xlsx
  Состав файлов:

   
