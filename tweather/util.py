import os
from darksky.api import DarkSky
from darksky.types import weather
from geopy.geocoders import Nominatim
import tweepy
import time
import re

class WeatherClient:
    def __init__(self, token):
        self.ds = DarkSky(token)
        self.geolocator = Nominatim(user_agent="tweather")
        self.weather_data = {}

    def get_lat_long(self, place):
        """
        Given a location  from Twitter, generate lat and long
        :param place: Name of location (Manhattan)
        :return: lat, long tuple
        """
        try:
            location = self.geolocator.geocode(place)
            return round(location.latitude, 4), round(location.longitude, 4)
        except Exception as e:
            print(e)
            return None, None

    def reverse_geocode(self, lat, long):
        try:
            location = self.geolocator.reverse(f"{lat}, {long}")
            return location.address
        except Exception as e:
            print(e)
            return None

    def get_weather_data(self, lat, long, created_at):
        """
        Check if I have that place in the weather dict
        If yes, have I seen it in the last hour?
            If yes, use that data to preserve api calls
        Else
            Make the api call and save the time and the weather data
            return the weather data for the db
        :param lat:
        :param long:
        :return:
        """
        pass
        one_hour = 3600
        key = f"{lat}{long}"
        if self.weather_data.get(key):
            # then we check if its time is in the last hour
            time_delta = created_at - self.weather_data.get(key)['time']
            if time_delta <= one_hour:
                # then we can reuse the data and save API calls
                return self.weather_data.get(key)['weather']
        else:
            # we need to get weather data for this lat long
            weather_data = self._get_weather_data(lat, long)
            if weather_data:
                # then we can save it for reuse!
                self.weather_data[key] = {
                    "time": created_at,
                    "weather": weather_data
                }
            return weather_data

    def _get_weather_data(self, lat, long):
        """
        Given a location, get the current weather conditions.
        :param lat: latitude
        :param long: longitude
        :return: current weather conditions
        """
        return {}
        try:
            # get the data
            forecast = self.ds.get_forecast(
                lat, long,
                exclude=[weather.HOURLY, weather.MINUTELY,
                         weather.DAILY, weather.ALERTS, weather.FLAGS])

            # add lat & long to the hourly weather data for composite key in db
            data = forecast.currently
            data.latitude = lat
            data.longitude = long
            data = data.__dict__
            data.pop("time")
            return data
        except Exception as e:
            print(e)
            return None


def has_keyword(tweet, keywords):
    """
    Given a tweet, does it have one of the keywords?
    True if yes, else False
    """
    temp = tweet.lower()
    for keyword in keywords:
        if bool(re.search(f'\\b{keyword}\\b', temp)):
            return True
    return False


def get_keywords(path):
    return open(path).read().splitlines()


def twitter_created_at_to_epoch(created_at):
    """
    Converts twitters created at to epoch time
    'Wed Oct 10 20:19:24 +0000 2018' -> 1539227964
    :param created_at:
    :return: epoch time as int
    """
    try:
        return int(time.mktime(time.strptime(
            created_at,"%a %b %d %H:%M:%S +0000 %Y")))
    except ValueError as e:
        print(f"something went wrong: {e}")
        return None


def get_tweet_text(status):
    # retweet check
    if hasattr(status, "retweeted_status"):
        try:
            tweet = status.retweeted_status.extended_tweet['full_text']
        except AttributeError:  # extended tweet DNE
            tweet = status.retweeted_status.text
    else:
        try:
            tweet = status.extended_tweet['full_text']
        except AttributeError:  # extended tweet DNE
            tweet = status.text
    return tweet


def init_tweepy():
    consumer_key = os.getenv("twitter_consumer_key")
    consumer_secret = os.getenv("twitter_consumer_secret")
    access_token = os.getenv("twitter_access_token")
    access_token_secret = os.getenv("twitter_access_token_secret")
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    return tweepy.API(auth, wait_on_rate_limit=True,
                      wait_on_rate_limit_notify=True)

