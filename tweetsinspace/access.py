import json
from zipfile import ZipFile

def get_tweets(date_str, line_nums, data_dir = "../data/tweets/"):
    y, m, d, hh, mm, ss = tuple(date_str.split("-"))

    if not data_dir.endswith("/"):
        data_dir += "/"

    zipfilepath = data_dir + "/".join(y, m, d, hh, mm) + ".zip"

    tweets = {}
    try:
        zipfile = ZipFile(zipfilepath, "r")

        json_name = date_str + ".json"
        file_name = mm + "/" + json_name

        tweet_texts = [line for i, line in enumerate(zipfile.open(file_name).readlines()) if i in line_nums]

        for tweet_text in tweet_texts:
            try:
                tweet = json.loads(tweet_text.decode("utf-8"))
                tweet_id = tweet["id_str"]

                tweets[tweet_id] = tweet
            except ValueError:
                continue

    except FileNotFoundError:
        continue

    return tweets
