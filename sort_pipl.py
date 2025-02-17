from dotenv import load_dotenv
import os
import pandas as pd
import shutil
pd.set_option("expand_frame_repr", False)
pd.set_option('display.max_colwidth', None)
from sqlalchemy import create_engine
from datetime import datetime
class SortPipl:
    def __init__(self):
        load_dotenv()
        # Записываем в переменные
        self.file_path_pipl = 'pipl.csv'
        # Убрал в комментарий, т.к. было лень ждать загрузки с интернета
        self.path_sales = os.getenv('path_sales')
        # Если что-то сменится, то лучше удалить
        self.path_sales = 'Сводная ИМ.xlsx'
        self.host = os.getenv('HOST')
        self.port = os.getenv('PORT')
        self.database = os.getenv('DATABASE_NAME')
        self.user = os.getenv('LOGIN')
        self.password = os.getenv('PASS')
        # Запрос для первого круга предложений
        self.query_first_krug = """SELECT 
    spr_client.createdate,
    spr_client.push_id
FROM 
    spr_client
LEFT JOIN 
    spr_check ON spr_client.card_number = spr_check.card_number
WHERE 
    spr_client.push_id NOT IN (
        SELECT DISTINCT spr_client.push_id
        FROM spr_client
        JOIN spr_check ON spr_client.card_number = spr_check.card_number
        JOIN sales_im ON spr_check.id_check = sales_im.id_check
    )
    AND spr_client.createdate > CURRENT_DATE - INTERVAL '61 days'
GROUP BY 
    spr_client.createdate, 
    spr_client.push_id;

"""
        # Запрос для второго круга уведомлений
        self.query_second_krug = """SELECT 
    spr_client.createdate,
    spr_client.push_id
FROM 
    spr_client
LEFT JOIN 
    spr_check ON spr_client.card_number = spr_check.card_number
WHERE 
    spr_client.push_id NOT IN (
        SELECT DISTINCT spr_client.push_id
        FROM spr_client
        JOIN spr_check ON spr_client.card_number = spr_check.card_number
        JOIN sales_im ON spr_check.id_check = sales_im.id_check
    )
    AND spr_client.createdate < CURRENT_DATE - INTERVAL '60 days'
    AND spr_client.createdate > CURRENT_DATE - INTERVAL '120 days'
GROUP BY 
    spr_client.createdate, 
    spr_client.push_id;
"""
        # Итоговая переменная подключения
        self.conn_to_db = f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
    def start_first_krug(self):
        """Вызов функций для первого круга акций 1-60 дней"""
        print('Работа с бд')
        # Беру данные с базы данных и немного их модифицирую
        df = self.__take_data_for_DB(self.query_first_krug)
        print(df)
        # Получаем значения с таблицы
        print("Работа с таблицей")
        df_aktzii = self.__take_table()
        print(df_aktzii)
        # Соединяем датасет с пользователями со значениями из датасета с акциями
        print('Соединение БД с таблицей')
        df_itog = self.__take_aktzii(df, df_aktzii)

        print("Сохранение")
        print(df_itog['Значение'].unique())
        # Убираем строки с пустыми значениями(этим мы не предложим скидочки)
        df_itog = df_itog.dropna(subset=['Значение'])
        # Группируем данные по столбцу "Акции"
        grouped = df_itog.groupby('Акции')

        #Проверка и создание основной папки
        output_folder = 'акции_данные'
        if not os.path.exists(output_folder):
            os.makedirs(output_folder)
        # Проверяем, существует ли папка
        if os.path.exists(output_folder):
            # Удаляем всё содержимое папки
            shutil.rmtree(output_folder)
            # Создаем пустую папку с тем же именем
            os.makedirs(output_folder)
            print(f"Содержимое папки {output_folder} успешно удалено.")
        else:
            print(f"Папка {output_folder} не существует.")


        # Создание папок
        for i in range(60, 0, -3):  # Проверяем столбцы с 60 до 1 дня
            # Если находятся совпадения, получаем имя столбца, с которым будем работать
            name = f"{i} день"
            if i == 3:
                name2 = f"{1} день"
                folder_path = os.path.join(output_folder, name2)
                if not os.path.exists(folder_path):
                    os.makedirs(folder_path)
            folder_path = os.path.join(output_folder, name)
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)

        # Сохраняем данные в отдельные файлы
        for name, group in grouped:
            # Создаем папку для каждой акции, если она еще не существует
            folder_path = os.path.join(output_folder, name)
            if not os.path.exists(folder_path):
                os.makedirs(folder_path)
            for i in range(1, 6):
                file_path = os.path.join(folder_path, f'Аудитория {i}.csv')
                # Делим на аудитории
                df_itog1 = group[group['Аудитория'] == i]
                # Здесь можно было бы убрать лишние столбцы(если надо)
                col = 'ID пользователя\t"Имя пользователя"\t"Дата проникновения в аудиторию"\t"Дата исчезновения из аудитории"\t"Номер телефона пользователя"\t"Номер карты пользователя"'

                itog = pd.DataFrame(columns=[col])
                itog[col] = df_itog1['push_id']
                itog.to_csv(file_path, index=False)
        print('Данные успешно обновлены')
            #group.to_csv(file_path, index=False)




        # Сохраняем данные

    def start_second_krug(self):
        """Функция для второго круга акций. 60-120 дней"""
        df = self.__take_data_for_DB_second(self.query_second_krug)

        df_aktzii = self.__take_table2()
        df_itog = self.__take_aktzii_second(df, df_aktzii)
        df_itog = df_itog.dropna(subset=['Значение'])
        df_itog1 = df_itog[df_itog['Аудитория'] == 1]
        df_itog2 = df_itog[df_itog['Аудитория'] == 2]
        df_itog3 = df_itog[df_itog['Аудитория'] == 3]
        df_itog4 = df_itog[df_itog['Аудитория'] == 4]
        df_itog5 = df_itog[df_itog['Аудитория'] == 5]
        df_itog1.to_excel('Itog1_second.xlsx', index=False)
        df_itog2.to_excel('Itog2_second.xlsx', index=False)
        df_itog3.to_excel('Itog3_second.xlsx', index=False)
        df_itog4.to_excel('Itog4_second.xlsx', index=False)
        df_itog5.to_excel('Itog5_second.xlsx', index=False)
    def __take_data_for_DB(self, query):
        """Получаем данные с БД. Добавляем Аудитории и фильтруем строки
         :query: Запрос SQL в виде строки"""
        # Создаем строку подключения
        connection_string = self.conn_to_db
        # Создаём объект Engine
        engine = create_engine(connection_string)
        # SQL-запрос для извлечения данных
        df = pd.read_sql_query(query, engine)
        # Завершаем подключение к БД
        engine.dispose()

        # Преобразуем столбец createdate в формат datetime
        df['createdate'] = pd.to_datetime(df['createdate'])

        # Получаем сегодняшнюю дату как pandas datetime
        today = pd.to_datetime(datetime.today().date())

        # Вычисляем разницу в днях и добавляем в новый столбец 'days_difference'
        df['Количество дней'] = (today - df['createdate']).dt.days
        # Новички нам не интересны
        df = df[df['Количество дней'] != 0]
        # Записываем в переменную тех, с которыми начинаем работать
        df_new_pipl = df[df['Количество дней'] == 1]['push_id']
        # Добавляем этих людей в список
        self.__add_pipl(df_new_pipl)
        # Создаём Аудитории
        df['Аудитория'] = df['push_id'] % 5 + 1

        # Проводим фильтрацию. Оставляем только те строки, которые есть в нашем списке "новичков"
        df = self.__filt_new_pipl(df)
        return df
    def __take_data_for_DB_second(self, query):
        """Получаем данные с БД. В columns_list должны быть сначала str столбец, а потом id"""
        # Создаем строку подключения
        connection_string = self.conn_to_db
        # Создаём объект Engine
        engine = create_engine(connection_string)
        # SQL-запрос для извлечения данных
        df = pd.read_sql_query(query, engine)
        # Завершаем подключение к БД
        engine.dispose()

        # Преобразуем столбец createdate в формат datetime
        df['createdate'] = pd.to_datetime(df['createdate'])

        # Получаем сегодняшнюю дату как pandas datetime
        today = pd.to_datetime(datetime.today().date())

        # Вычисляем разницу в днях и добавляем в новый столбец 'days_difference'
        df['Количество дней'] = (today - df['createdate']).dt.days
        df['Аудитория'] = df['push_id'] % 5 + 1
        df = self.__filt_new_pipl(df)
        return df
    def __take_table(self):
        """"""
        # Считываем страницу Гипотезы для цепочки 0 заказов и пропускаем 1 строку.
        df = pd.read_excel(self.path_sales, skiprows=1, sheet_name="Гипотезы для цепочки 0 заказов")  # Пропускаем первую строку (индекс 0)
        # Берём первую строку и записываем её в переменную
        df2 = df.iloc[0]
        # Обрезаем DataFrame, чтобы получить только строки с 2 по 7
        df = df.iloc[1:6]  # Индексы 1 до 6
        # Переименовываем названия столбцов
        df.columns = df2.values
        # Добавляем к каждой строке по значению аудитории
        df['Аудитория'] = [1, 2, 3, 4, 5]
        return df
    def __take_table2(self):
        # Берём вторую часть таблицы
        df = pd.read_excel(self.path_sales, skiprows=1, sheet_name="Гипотезы для цепочки 0 заказов")  # Пропускаем первую строку (индекс 0)
        df2 = df.iloc[11]
        # Обрезаем DataFrame, чтобы получить только строки с 2 по 8 (индексы 1-7)
        df = df.iloc[12:17]  # Индексы 1 до 8
        df.columns = df2.values
        print(df)
        df['Аудитория'] = [1, 2, 3, 4, 5]
        return df
    def __take_aktzii(self, df_data, df_aktsii):
        """Соединяем данные.
        :df_data: датасет с данные о пользователях
        :df_aktsii: датасет с акциями"""
        # Создаём пустой столбец и по мере пополнения наполняем его
        df_data['Значение'] = None
        df_data['Акции'] = None
        # Проходим по строкам в датасете Data
        for index, row in df_data.iterrows():
            days = row['Количество дней']

            # Находим соответствующий столбец в датасете Акции
            column_name = None
            for i in range(60, 0, -3):  # Проверяем столбцы с 60 до 1 дня
                # Если находятся совпадения, получаем имя столбца, с которым будем работать
                if days == i:
                    column_name = f"{i} день"
                elif days == 1:
                    column_name = f"{1} день"
                # Это если нам нужно дарить не по дням(3,6,9 и т.д.), а по периодам(с 3 по 6)
                """elif (days < i and days > i - 3) or days == i:
                    column_name = f"{i} день"
                    break"""

            # Если найден соответствующий столбец, присваиваем значение
            if column_name:
                if column_name == None:
                    df_data.at[index, 'Значение'] = None
                # Находим значение по аудитории
                if column_name != None:
                    value = df_aktsii.loc[df_aktsii['Аудитория'] == row['Аудитория'], column_name].values[0]
                    df_data.at[index, 'Значение'] = value
                    df_data.at[index, 'Акции'] =  column_name
        return df_data
    def __take_aktzii_second(self, df_data, df_aktsii):

        df_data['Значение'] = None

        # Проходим по строкам в датасете Data
        for index, row in df_data.iterrows():
            days = row['Количество дней']

            # Находим соответствующий столбец в датасете Акции
            column_name = None
            for i in range(60, 0, -3):  # Проверяем столбцы с 60 до 1 дня


                if days / 2 == i:
                    column_name = f"{i} день"

                """elif (days < i and days > i - 3) or days == i:
                    column_name = f"{i} день"
                    break"""

            # Если найден соответствующий столбец, присваиваем значение
            if column_name:
                if column_name == None:
                    df_data.at[index, 'Значение'] = None
                # Находим значение по аудитории
                if column_name != None:
                    value = df_aktsii.loc[df_aktsii['Аудитория'] == row['Аудитория'], column_name].values[0]
                    df_data.at[index, 'Значение'] = value
        return df_data
    def __take_prov(self, df):
        """Получаем данные с БД."""
        # Создаем строку подключения
        connection_string = self.conn_to_db
        # Создаём объект Engine
        engine = create_engine(connection_string)
        # SQL-запрос для извлечения данных
        query = f"""SELECT 
    spr_client.createdate,
    spr_client.push_id,
    spr_check.id_check 
FROM 
    spr_client
 JOIN 
    spr_check ON spr_client.card_number = spr_check.card_number
 JOIN 
    sales_im ON spr_check.id_check  = sales_im.id_check;

        """

        df_check = pd.read_sql_query(query, engine)
        # Находим пересечения по столбцу push_id
        intersection = pd.merge(df, df_check, on='push_id')
        # Выводим результат
        print("Пересечения по столбцу push_id:")
        print(intersection)
    def __add_pipl(self, new_data):
        # Записываем переменную пути к файлу.
        csv_file_path = self.file_path_pipl

        # Проверяем, существует ли файл
        if os.path.exists(csv_file_path):
            # Если файл существует, считываем его
            df = pd.read_csv(csv_file_path)
        else:
            # Если файл не существует, создаём новый DataFrame
            df = pd.DataFrame(columns=['push_id'])  # Создаём DataFrame с нужными столбцами

        # Объединяем старый и новый DataFrame
        new_data_df = new_data.to_frame(name='push_id')  # Преобразуем Series в DataFrame с именем столбца
        df = pd.concat([df, new_data_df], ignore_index=True)
        # Убираем дубликаты
        df = df.drop_duplicates(subset='push_id', keep='first').reset_index(drop=True)
        # Сохраняем обновлённый DataFrame обратно в CSV файл
        df.to_csv(csv_file_path, index=False)
    def __filt_new_pipl(self, main_df):
        """Фильтруем данные по шаблону из файла
        :main_df: датасет, который надо фильтровать"""
        # Считываем файл
        pipli_new = pd.read_csv(self.file_path_pipl)
        # Фильтруем основной датасет по push_id
        filtered_dataset = main_df[main_df['push_id'].isin(pipli_new['push_id'])]
        return filtered_dataset
