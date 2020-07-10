from tweather.util import *
from dotenv import load_dotenv
from urllib3.exceptions import ReadTimeoutError
import json
import os

load_dotenv()

US = [-126.210938, 24.686952, -66.708984, 50.457504]


class Listener(tweepy.StreamListener):
    def __init__(
            self,
            weather_client: WeatherClient,
            keywords: list
    ):
        super(Listener, self).__init__()
        self.weather_client = weather_client
        self.keywords = keywords
        self.count = 0

    def on_status(self, status):

        tweet = get_tweet_text(status)

        if has_keyword(tweet, self.keywords):
            tweather_entry = self.prepare_data(status)
            if tweather_entry:
                tweather_entry["tweet"] = tweet
                print(json.dumps(tweather_entry, indent=2, sort_keys=True))
                self.count += 1

    def on_error(self, status_code):
        print(status_code)
        return False

    def prepare_data(self, status):

        if status.coordinates:
            lat = round(status.coordinates['coordinates'][1], 4)
            long = round(status.coordinates['coordinates'][0], 4)
            place = self.weather_client.reverse_geocode(lat, long)
        else:
            lat, long = self.weather_client.get_lat_long(
                status.place.full_name)
            place = status.place.full_name
        if lat and long and place:
            epoch_time = int(status.created_at.timestamp())
            tweather_data = self.weather_client.get_weather_data(
                lat, long, epoch_time)

            # Add the data to the weather_data json
            tweather_data["epoch"] = epoch_time
            tweather_data["location"] = place
            tweather_data["id"] = status.id_str

            return tweather_data
        else:
            return None


def start_stream(listener):

    api = init_tweepy()
    my_stream = tweepy.Stream(auth=api.auth, listener=listener)
    start = time.time()

    # start stream
    try:
        my_stream.filter(locations=US)
    except ReadTimeoutError:
        total_seconds = time.time() - start
        print(f"{listener.count} tweets in {total_seconds} seconds")
        print(f"{listener.count / total_seconds * 60} tweets per minute")
        print('Done.')
        my_stream.disconnect()


if __name__ == "__main__":
    # Init everything
    weather_client = WeatherClient(os.getenv("d_token"))
    keywords = get_keywords("tweather/keywords.txt")

    # Create tweet listener
    listener = Listener(weather_client, keywords)

    try:
        while True:
            start_stream(listener)
    except KeyboardInterrupt as e:
        print("Stopped.")
