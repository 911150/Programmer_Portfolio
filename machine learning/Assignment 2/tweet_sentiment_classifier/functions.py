import re
import string

import nltk
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix, classification_report

import pandas as pd
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer
from sklearn.metrics import ConfusionMatrixDisplay
import matplotlib.pyplot as plt

stopwords_ = set(stopwords.words('english'))
punc_list_ = string.punctuation
tokenizer_ = RegexpTokenizer(r'\w+')
stemmer_ = nltk.PorterStemmer()
lemmatizer_ = nltk.WordNetLemmatizer()

rpt_regex_ = re.compile(r"(.)\1{1,}", re.IGNORECASE)

# Dictionary for emojis -> From kaggle
emojis = {':)': 'smile', ':-)': 'smile', ';d': 'wink', ':-E': 'vampire', ':(': 'sad',
          ':-(': 'sad', ':-<': 'sad', ':P': 'raspberry', ':O': 'surprised',
          ':-@': 'shocked', ':@': 'shocked', ':-$': 'confused', ':\\': 'annoyed',
          ':#': 'mute', ':X': 'mute', ':^)': 'smile', ':-&': 'confused', '$_$': 'greedy',
          '@@': 'eyeroll', ':-!': 'confused', ':-D': 'smile', ':-0': 'yell', 'O.o': 'confused',
          '<(-_-)>': 'robot', 'd[-_-]b': 'dj', ":'-)": 'sadsmile', ';)': 'wink',
          ';-)': 'wink', 'O:-)': 'angel', 'O*-)': 'angel', '(:-D': 'gossip', '=^.^=': 'cat'
          ,'(:': 'smile'}


# noinspection PyShadowingNames,PyPep8Naming
def preprocess_tweets(tweet):
    # make tweet lower case
    tweet = tweet.lower()

    # remove stopwords
    tweet = remove_stopwords(tweet)

    # Convert www.* or https?://* to URL
    tweet = convert_urls(tweet)

    # Convert emojis
    tweet = convert_emojies(tweet)

    # Convert @username to __HANDLE
    tweet = convert_usernames(tweet)

    # tweet = tweet.strip('\'"')
    # in english '/' can be use as 'or', so replace it with a space
    tweet = tweet.replace('/', ' ')

    # remove punctuations
    tweet = clean_punctuation(tweet)

    # Replace #word with word
    tweet = convert_hashtags(tweet)

    # Remove numerics
    tweet = clean_numeric(tweet)

    # Clean repeating words like happyyyyyyyy
    # tweet = cleaning_repeating_char(tweet)
    tweet = clean_repeated_char(tweet)

    # Get tokenization
    tweet = tokenize_tweet(tweet)

    # apply stemming
    tweet = stem(tweet)

    # apply lemmatization
    tweet = lemmatize(tweet)

    # print(" ".join(tweet))
    return " ".join(tweet)


def tts(x, y, p):
    x_train, x_test, y_train, y_test = train_test_split(x, y, train_size=p)
    return x_train, x_test, y_train, y_test


def toLabel(df, column):
    df[column] = df[column].replace([1, -1, 0], ['positive', 'negative', 'neutral'])
    pass


def toNum(df, column):
    df[column] = df[column].replace(['positive', 'negative', 'neutral'], [1, -1, 0])
    return df


def remove_stopwords(tweet):
    tweet = " ".join([word for word in str(tweet).split() if word not in stopwords_])
    return tweet


def clean_punctuation(tweet):
    translator = str.maketrans('', '', punc_list_)
    return tweet.translate(translator)


# Convert @username to __HANDLE ||| OR ||| remove it completely
def convert_usernames(tweet):
    # return re.sub('@[^\s]+', '__HANDLE', tweet)
    return re.sub('@[^\s]+', 'USER', tweet)


# Replace #word with word
def convert_hashtags(tweet):
    return re.sub(r'#([^\s]+)', r'\1', tweet)


# Convert www.* or https?://* to URL |||| OR |||| remove the URL completely, TODO lets start with that
def convert_urls(tweet):
    return re.sub('((www\.[^\s]+)|(https?://[^\s]+))', 'URL', tweet)
    # return re.sub('((www\.[^\s]+)|(https?://[^\s]+))', 'URL', tweet)


def convert_emojies(tweet):
    for emoji in emojis:
        tweet = tweet.replace(emoji, "EMOJI" + emojis[emoji])
    return tweet


# Remove numeric numbers from tweet
def clean_numeric(tweet):
    return re.sub('[0-9]+', '', tweet)


# Remove repeated letters
def clean_repeated_char(tweet):
    tweet = rpt_regex_.sub(r"\1\1", tweet)
    return tweet


# Not sure if i will use this or not
def cleaning_repeating_char(tweet):
    return re.sub(r'(.)\1+', r'\1', tweet)


# convert the tweet into tokens
def tokenize_tweet(tweet):
    return tokenizer_.tokenize(tweet)


# apply stemming to the tweet
def stem(tweet):
    return [stemmer_.stem(word) for word in tweet]


# lemmatize the tweet
def lemmatize(tweet):
    return [lemmatizer_.lemmatize(word) for word in tweet]


# TODO this does .lower twice, but it lowers the handle things once
def alternative_stem(tweet):
    words = [word if (word[0:2] == '__') else word.lower()
             for word in tweet.split()
             if len(word) >= 3]
    words = [stemmer_.stem(w) for w in words]
    # tweet_stem = ''
    # tweet_stem = ' '.join(words)
    return words


def get_train_data(X, y, test_size):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size)
    return X_train, X_test, y_train, y_test


def eval_model(model, X_train, y_train, X_test, y_test):
    accuracy_train = model.score(X_train, y_train)
    accuracy_test = model.score(X_test, y_test)

    y_pred = model.predict(X_test)

    # Print evaluation metrics
    print(classification_report(y_test, y_pred))

    conf_matrix = confusion_matrix(y_test, y_pred, labels=model.classes_)

    # set normalize == true for percentages
    disp = ConfusionMatrixDisplay(confusion_matrix=conf_matrix,
                                  display_labels=model.classes_)

    disp.plot(cmap='blues')
    plt.show()

# twt = " is anybody going to the radio station tomorrow to see shawn? me and my friend may go but we would like to make new friends/meet there (:	"
# preprocess_tweets(twt)


# %%
