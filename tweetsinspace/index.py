from sqlitedict import SqliteDict
from datetime import datetime, timedelta

def get_user_tweets(user_id, time_window = None, index_dir = "../data/index/"):
    """
    time_window should be a tuple of two mm/dd/yy strings denoting start and end days
    """
    index = SqliteDict(index_dir + "users.db")

    if time_window:
        start, stop = time_window

        start = datetime.strptime(start, "%m/%d/%y")
        stop = datetime.strptime(stop, "%m/%d/%y") + timedelta(days = 1)

        total_seconds = int((stop - start).total_seconds())
        date_strs = []

        for second in range(total_seconds):
            time = start + timedelta(seconds = second)
            date_strs.append(time.strftime("%y-%m-%d-%H-%M-%S"))

        complete_record = index[user_id]

        filtered_record = {date_str: complete_record[date_str] for date_str in complete record if date_str in date_strs}

        return filtered_record

    else:
        return index[user_id]

def get_language_tweets(lang_id, time_window = None, index_dir = "../data/index/"):
    """
    time_window should be a tuple of two mm/dd/yy strings denoting start and end days
    """
    index = SqliteDict(index_dir + "languages.db")

    if time_window:
        start, stop = time_window

        start = datetime.strptime(start, "%m/%d/%y")
        stop = datetime.strptime(stop, "%m/%d/%y") + timedelta(days = 1)

        total_seconds = int((stop - start).total_seconds())
        date_strs = []

        for second in range(total_seconds):
            time = start + timedelta(seconds = second)
            date_strs.append(time.strftime("%y-%m-%d-%H-%M-%S"))

        complete_record = index[lang_id]

        filtered_record = {date_str: complete_record[date_str] for date_str in complete record if date_str in date_strs}

        return filtered_record

    else:
        return index[lang_id]

def get_language_tweets(place_id, time_window = None, index_dir = "../data/index/"):
    """
    time_window should be a tuple of two mm/dd/yy strings denoting start and end days
    """
    index = SqliteDict(index_dir + "places.db")

    if time_window:
        start, stop = time_window

        start = datetime.strptime(start, "%m/%d/%y")
        stop = datetime.strptime(stop, "%m/%d/%y") + timedelta(days = 1)

        total_seconds = int((stop - start).total_seconds())
        date_strs = []

        for second in range(total_seconds):
            time = start + timedelta(seconds = second)
            date_strs.append(time.strftime("%y-%m-%d-%H-%M-%S"))

        complete_record = index[place_id]

        filtered_record = {date_str: complete_record[date_str] for date_str in complete record if date_str in date_strs}

        return filtered_record

    else:
        return index[place_id]

def index_tweet(tweet, line_num, date_str, user_index, language_index, place_index, index_dir = "../data/index/"):
    """
    yield index dicts for a single tweet
    """

    tweet_id = tweet["id_str"]

    user_id = tweet["user"]["id_str"]
    user_index[user_id][date_str].append((line_num, tweet_id))

    language = tweet["lang"]
    language_index[language][date_str].append((line_num, tweet_id))

    if "coordinates" in tweet:
        if "place" in tweet:
            if tweet["place"]:
                place_index[tweet["place"]["id"]][date_str].append((line_num, tweet_id))

    return user_index, language_index, place_index

def write_index(index_update, index_name, index_dir = "../data/index/"):
    """
    write index to disk
    index_name should be 'users', 'languages', or 'places'
    """
    index = SqliteDict(index_dir + index_name + ".db")

    for key in index_update:
        if key in index:
            for date_str in index_update[key]:
                if date_str in index[key]:
                    index[key][date_str].extend(index_update[key][date_str])
                else:
                    index[key][date_str] = index_update[key][date_str]
        else:
            index[key] = index_update[key]

    index.commit()
