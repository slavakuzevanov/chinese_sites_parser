import gc
import datetime
import os
import psutil
import pymysql
import telebot
from telebot import types

from parser_classes import SpaceChinaParser, jqkaParser, SpaceFlightsFansParser, sipprParser, TiebaBaiduParser

DB_USER = <DB_USER>
DB_PASSWORD = <DB_PASSWORD>
DB_HOST = <DB_HOST>
DB_LINK = <DB_LINK>


def create_r_search_string(list_of_key_words):
    return r'|'.join(map(lambda x: f"({x})", list_of_key_words))


token = <telegram_bot_token>
START, WORDS, PARSER_CHOOSE, PARSING_IN_PROGRESS = range(4)
DELETE_KEY_WORDS = 100
VIEW_KEY_WORDS = 101
VIEW_MEMORY_USAGE = 102

SITES_PARSERS = {'China Aerospace Science and Technology Corporation': SpaceChinaParser,
                 'Space Flight Fans': SpaceFlightsFansParser, '10jqka': jqkaParser,
                 'SIPPR': sipprParser, 'Tieba Baidu': TiebaBaiduParser}
OPTION_BUTTONS = {'ADD NEW KEY WORD': WORDS, 'CHOOSE SITE TO PARSE': PARSER_CHOOSE,
                  'VIEW KEY WORDS': VIEW_KEY_WORDS, 'DELETE ALL KEY WORDS': DELETE_KEY_WORDS,
                  'VIEW MEMORY USAGE': VIEW_MEMORY_USAGE}


def create_r_search_string(list_of_key_words):
    return r'|'.join(map(lambda x: f"({x})", list_of_key_words))


def create_new_user(message):
    try:
        cnx = pymysql.connect(user=DB_USER, password=DB_PASSWORD,
                              host=DB_HOST,
                              database=DB_LINK)
        cursor = cnx.cursor()
        add_user = ("INSERT INTO user"
                    "(id, state, insert_date, change_date)"
                    "VALUES (%s, %s, %s, %s)")
        data_user = (message.chat.id, START, datetime.datetime.now(), datetime.datetime.now())
        # Insert new user
        cursor.execute(add_user, data_user)
        # Make sure data is committed to the database
        cnx.commit()
        cursor.close()
        cnx.close()
    except:
        pass


def get_state(message):
    try:
        cnx = pymysql.connect(user=DB_USER, password=DB_PASSWORD,
                              host=DB_HOST,
                              database=DB_LINK)
        cursor = cnx.cursor()
        query = "SELECT * FROM user WHERE id=%s"
        id = message.chat.id
        cursor.execute(query, id)
        user_state = cursor.fetchone()[1]
        cursor.close()
        cnx.close()
        return user_state
    except:
        pass


def update_state(message, state):
    try:
        cnx = pymysql.connect(user=DB_USER, password=DB_PASSWORD,
                              host=DB_HOST,
                              database=DB_LINK)
        cursor = cnx.cursor()
        id = message.chat.id
        query = "UPDATE user SET state = %s, change_date = %s WHERE id = %s"
        values = (state, datetime.datetime.now(), id)
        cursor.execute(query, values)
        cnx.commit()
        cursor.close()
        cnx.close()
    except:
        pass


def update_state_after_crash():
    try:
        cnx = pymysql.connect(user=DB_USER, password=DB_PASSWORD,
                              host=DB_HOST,
                              database=DB_LINK)
        cursor = cnx.cursor()
        query = "UPDATE user SET state = %s, change_date = %s"
        values = (START, datetime.datetime.now())
        cursor.execute(query, values)
        cnx.commit()
        cursor.close()
        cnx.close()
    except:
        pass


def get_users_chat_id_after_crash():
    try:
        cnx = pymysql.connect(user=DB_USER, password=DB_PASSWORD,
                              host=DB_HOST,
                              database=DB_LINK)
        cursor = cnx.cursor()
        query = "SELECT id from user where state = %s"
        values = PARSING_IN_PROGRESS
        cursor.execute(query, values)
        results = cursor.fetchall()
        users_id = [_[0] for _ in results]
        cnx.commit()
        cursor.close()
        cnx.close()
        return users_id
    except:
        pass


def add_key_word_for_user(message, key_word: str):
    try:
        cnx = pymysql.connect(user=DB_USER, password=DB_PASSWORD,
                              host=DB_HOST,
                              database=DB_LINK)
        cursor = cnx.cursor()
        add_user = ("INSERT INTO user_key_word"
                    "(user_id, key_word, insert_date)"
                    "VALUES (%s, %s, %s)")
        data_user = (message.chat.id, key_word, datetime.datetime.now())
        # Insert new user
        cursor.execute(add_user, data_user)
        # Make sure data is committed to the database
        cnx.commit()
        cursor.close()
        cnx.close()
    except:
        bot.send_message(message.chat.id, text='Something wrong')


def list_current_key_words(message):
    try:
        cnx = pymysql.connect(user=DB_USER, password=DB_PASSWORD,
                              host=DB_HOST,
                              database=DB_LINK)

        cursor = cnx.cursor()
        query = 'SELECT key_word FROM user_key_word WHERE user_id = %s ORDER BY insert_date ASC'

        cursor.execute(query, message.chat.id)
        key_words = cursor.fetchall()
        cnx.commit()
        cursor.close()
        cnx.close()

        return key_words
    except:
        return None


def delete_users_key_words(message):
    cnx = pymysql.connect(user=DB_USER, password=DB_PASSWORD,
                          host=DB_HOST,
                          database=DB_LINK)

    cursor = cnx.cursor()

    query = "DELETE FROM user_key_word WHERE user_id = %s"

    cursor.execute(query, message.chat.id)

    cnx.commit()
    cursor.close()
    cnx.close()


bot = telebot.TeleBot(token)


@bot.message_handler(commands=['start'])
def handle_confirmation(message):
    try:
        create_new_user(message)
    except:
        pass

    bot.send_message(message.chat.id, text="I can help you get articles urls from chinese"
                                           " sites where key words found. \n"
                                           'You can control me by sending these commands:\n'
                                           '\n'
                                           '/options - display options\n'
                     )


def create_options_keyboard():
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    buttons = [types.InlineKeyboardButton(text=key, callback_data=value) for key, value in OPTION_BUTTONS.items()]
    keyboard.add(*buttons)
    return keyboard


def create_sites_keyboard():
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    buttons = [types.InlineKeyboardButton(text=key, callback_data=key) for key in SITES_PARSERS]
    keyboard.add(*buttons)
    return keyboard


@bot.message_handler(commands=['options'])
def handle_help(message):
    current_state = get_state(message)
    if current_state != PARSING_IN_PROGRESS:
        update_state(message, START)
        keyboard = create_options_keyboard()
        bot.send_message(message.chat.id, text="I can help you get articles urls from "
                                               "chinese sites where key words found. \n"
                                               'You can control me by using this buttons below\n',
                         reply_markup=keyboard)
    else:
        bot.send_message(message.chat.id, text='Please, wait! Parsing is in progress')


@bot.callback_query_handler(func=lambda call: call.data in [str(value) for key, value in OPTION_BUTTONS.items()])
def options_callback_handler(callback_query):
    message = callback_query.message
    text = callback_query.data
    current_state = get_state(message)
    if current_state in [START, WORDS] and int(text) == WORDS:
        bot.send_message(message.chat.id, text='Write new key word')
        update_state(message, WORDS)
    elif int(text) == VIEW_MEMORY_USAGE:
        process = psutil.Process(os.getpid())
        bot.send_message(message.chat.id, text=f'{process.memory_info().rss / (1024 ** 2)} MB')

    elif current_state in [START, WORDS, PARSER_CHOOSE] and int(text) == PARSER_CHOOSE:
        key_words = list_current_key_words(message)
        if len(key_words) == 0:
            bot.send_message(message.chat.id, text='You need to add at least one key word')
            update_state(message, START)
        else:
            sites_keyboard = create_sites_keyboard()
            bot.send_message(message.chat.id, text='Choose site to parse', reply_markup=sites_keyboard)
            update_state(message, PARSER_CHOOSE)
    elif current_state != PARSING_IN_PROGRESS and int(text) == DELETE_KEY_WORDS:
        delete_users_key_words(message)
        update_state(message, START)
        bot.send_message(message.chat.id, text='All key words have been deleted')
        bot.send_message(message.chat.id, text="Your next step?", reply_markup=create_options_keyboard())
    elif current_state != PARSING_IN_PROGRESS and int(text) == VIEW_KEY_WORDS:
        update_state(message, START)
        current_key_words = list_current_key_words(message)
        answer = 'Current key words:' + '\n'
        counter = 1
        for row in current_key_words:
            answer += f'{counter}: {row[0]}' + '\n'
            counter += 1
        bot.send_message(message.chat.id, text=answer)
        bot.send_message(message.chat.id, text="Your next step?", reply_markup=create_options_keyboard())
    else:
        bot.send_message(message.chat.id, text='Please, wait! Parsing is in progress')


@bot.message_handler(func=lambda message: get_state(message) == WORDS, content_types=['audio', 'video', 'document',
                                                                                      'text', 'location', 'contact',
                                                                                      'sticker', 'photo'])
def handle_add_new_key_word(message):
    print(message.content_type)
    if message.content_type == 'text':
        add_key_word_for_user(message, message.text)
        update_state(message, START)
        bot.send_message(message.chat.id, text='New key word added successfully')
        current_key_words = list_current_key_words(message)

        answer = 'Current key words:' + '\n'
        counter = 1
        for row in current_key_words:
            answer += f'{counter}: {row[0]}' + '\n'
            counter += 1
        bot.send_message(message.chat.id, text=answer)
        bot.send_message(message.chat.id, text="Your next step?", reply_markup=create_options_keyboard())
    else:
        bot.send_message(message.chat.id, text='This is not a text message. Please try again')


@bot.callback_query_handler(func=lambda call: call.data in [str(key) for key, value in SITES_PARSERS.items()])
def sites_callback_handler(callback_query):
    message = callback_query.message
    text = callback_query.data
    current_state = get_state(message)
    if current_state == PARSER_CHOOSE:
        try:
            update_state(message, PARSING_IN_PROGRESS)
            parser = SITES_PARSERS[text]()
            current_key_words = list_current_key_words(message)
            bot.send_message(message.chat.id, 'Parsing is in progress. This can take some time. Please, be patient. '
                                              'I will send you a csv file')
            parser.run(create_r_search_string([row[0] for row in current_key_words]))
            with open(f'{text}.csv', 'rb') as file:
                bot.send_document(message.chat.id, file)
            os.remove(f'{text}.csv')
            update_state(message, START)
            del parser
            gc.collect()
        except Exception as e:
            update_state(message, current_state)
            bot.send_message(message.chat.id, text=str(e))
            bot.send_message(message.chat.id, text='This parser is not implemented. Try another one')
            bot.send_message(message.chat.id, text='Choose site to parse', reply_markup=create_sites_keyboard())
    elif current_state == PARSING_IN_PROGRESS:
        bot.send_message(message.chat.id, text='Please, wait! Parsing is in progress')
    else:
        bot.send_message(message.chat.id, text='Try CHOOSE SITE TO PARSE command first')


user_id_after_crash = get_users_chat_id_after_crash()
for chat_id in user_id_after_crash:
    bot.send_message(chat_id, text='We are sorry. We had to restart the bot. Try running your parser again.')
update_state_after_crash()
bot.polling()
