import telebot
import json
import pandas as pd
import bson
import config


bot = telebot.TeleBot(config.TOKEN)


# Загрузка данных из BSON файла
def load_data(bson_file_path):
    with open(bson_file_path, 'rb') as f:
        data = bson.decode_all(f.read())
    df = pd.DataFrame(data)
    df['dt'] = pd.to_datetime(df['dt'])
    return df


# Агрегация данных
def aggregate_data(df, dt_from, dt_upto, group_type):
    dt_from = pd.to_datetime(dt_from)
    dt_upto = pd.to_datetime(dt_upto)
    mask = (df['dt'] >= dt_from) & (df['dt'] <= dt_upto)
    filtered_data = df.loc[mask]

    # Исключаем столбцы типа ObjectId перед агрегацией
    filtered_data = filtered_data.loc[:, ~filtered_data.columns.isin(['_id'])]

    # print("Columns in the filtered dataframe:", filtered_data.columns)

    if group_type == 'hour':
        filtered_data = filtered_data.set_index('dt').resample('H').sum()
    elif group_type == 'day':
        filtered_data = filtered_data.set_index('dt').resample('D').sum()
    elif group_type == 'month':
        filtered_data = filtered_data.set_index('dt').resample('ME').sum()
    else:
        raise ValueError("Invalid group_type. Expected 'hour', 'day', or 'month'.")

    labels = filtered_data.index.to_pydatetime().tolist()
    labels = [label.isoformat() for label in labels]
    return {'dataset': filtered_data['value'].tolist(), 'labels': labels}


# Загрузка данных
df = load_data('sample_collection.bson')


# Функция обработки сообщений
@bot.message_handler(content_types=['text'])
def lalala(message):
    try:
        request_data = json.loads(message.text)
        dt_from = request_data['dt_from']
        dt_upto = request_data['dt_upto']
        group_type = request_data['group_type']
        result = aggregate_data(df, dt_from, dt_upto, group_type)
        bot.send_message(message.chat.id, json.dumps(result))
    except Exception as e:
        bot.send_message(message.chat.id, f"Произошла ошибка: {str(e)}")


# Запуск бота
bot.polling(none_stop=True)
