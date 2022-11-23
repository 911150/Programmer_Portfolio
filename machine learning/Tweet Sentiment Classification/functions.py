import re
import string
import matplotlib.pyplot as plt

import nltk
from nltk.corpus import stopwords
from nltk.tokenize import RegexpTokenizer

from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix, classification_report
from sklearn.metrics import ConfusionMatrixDisplay


stopwords_ = set(stopwords.words('english'))
punc_list_ = string.punctuation
tokenizer_ = RegexpTokenizer(r'\w+')
stemmer_ = nltk.PorterStemmer()
lemmatizer_ = nltk.WordNetLemmatizer()

rpt_regex_ = re.compile(r"(.)\1{1,}", re.IGNORECASE)

# Dictionary for emojis, thankyou kaggle
emojis = {':)': 'smile', ':-)': 'smile', ';d': 'wink', ':-E': 'vampire', ':(': 'sad',
          ':-(': 'sad', ':-<': 'sad', ':P': 'raspberry', ':O': 'surprised',
          ':-@': 'shocked', ':@': 'shocked', ':-$': 'confused', ':\\': 'annoyed',
          ':#': 'mute', ':X': 'mute', ':^)': 'smile', ':-&': 'confused', '$_$': 'greedy',
          '@@': 'eyeroll', ':-!': 'confused', ':-D': 'smile', ':-0': 'yell', 'O.o': 'confused',
          '<(-_-)>': 'robot', 'd[-_-]b': 'dj', ":'-)": 'sadsmile', ';)': 'wink',
          ';-)': 'wink', 'O:-)': 'angel', 'O*-)': 'angel', '(:-D': 'gossip', '=^.^=': 'cat'
          ,'(:': 'smile'}



def preprocess_tweets(tweet):
    """Preprocesses a tweet"""
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

    # clean repeating words, e.g. 'happyyyyyyyy'
    tweet = clean_repeated_char(tweet)

    # get tokenization
    tweet = tokenize_tweet(tweet)

    # apply stemming
    tweet = stem(tweet)

    # apply lemmatization
    tweet = lemmatize(tweet)

    return " ".join(tweet)


def int_to_label(df, column):
    """Takes a dataframe and column of integers between -1 and 1 and maps these to their respective class"""
    df[column] = df[column].replace([1, -1, 0], ['positive', 'negative', 'neutral'])
    pass


def label_to_int(df, column):
    """Takes a dataframe and column of positive, negative and neutral classes and maps to integer scale"""
    df[column] = df[column].replace(['positive', 'negative', 'neutral'], [1, -1, 0])
    return df


def remove_stopwords(tweet):
    """Takes a string, returns the string without common stopwords"""
    tweet = " ".join([word for word in str(tweet).split() if word not in stopwords_])
    return tweet


def clean_punctuation(tweet):
    """Takes a string, returns the string without punctuation"""
    translator = str.maketrans('', '', punc_list_)
    return tweet.translate(translator)


def convert_usernames(tweet):
    """Takes a string, returns the string with @usernames replaced with generic handle"""
    # TODO: analyse converting @username to __HANDLE vs removing it completely?
    # return re.sub('@[^\s]+', '__HANDLE', tweet)
    return re.sub('@[^\s]+', 'USER', tweet)


def convert_hashtags(tweet):
    """Takes a string, returns the string without hashtags"""
    return re.sub(r'#([^\s]+)', r'\1', tweet)


def convert_urls(tweet):
    """Takes a string, returns the string with URLs replaced by generic handle"""
    return re.sub('((www\.[^\s]+)|(https?://[^\s]+))', 'URL', tweet)


def convert_emojies(tweet):
    """Takes a string, returns the string with emojis replaced by associated words"""
    for emoji in emojis:
        tweet = tweet.replace(emoji, "EMOJI" + emojis[emoji])
    return tweet


def clean_numeric(tweet):
    """Takes a string, returns the string without numerics"""
    return re.sub('[0-9]+', '', tweet)


def clean_repeated_char(tweet):
    """Takes a string, returns the string without repeating characters"""
    tweet = rpt_regex_.sub(r"\1\1", tweet)
    return tweet


def tokenize_tweet(tweet):
    """Takes a tweet, returns tokenized form"""
    return tokenizer_.tokenize(tweet)


def stem(tweet):
    """Takes a tweet, returns the stemmed form of the tweet"""
    return [stemmer_.stem(word) for word in tweet]


def lemmatize(tweet):
    """Takes a tweet, returns the lemmatized form of the tweet"""
    return [lemmatizer_.lemmatize(word) for word in tweet]


def get_train_data(X, y, test_size):
    """Split arrays or matrices into random train and test subsets (inherits docstring from sklearn)"""
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size)
    return X_train, X_test, y_train, y_test


def eval_model(model, X_train, y_train, X_test, y_test):
    """Evaluates a model given test data"""
    # get model predictions for the test/training sets
    y_pred = model.predict(X_test)
    #accuracy_train = model.score(X_train, y_train)
    #accuracy_test = model.score(X_test, y_test)

    # print evaluation metrics
    print(classification_report(y_test, y_pred))
    conf_matrix = confusion_matrix(y_test, y_pred, labels=model.classes_)

    # set normalize == true for percentages
    disp = ConfusionMatrixDisplay(confusion_matrix=conf_matrix,
                                  display_labels=model.classes_)

    disp.plot(cmap='blues')
    plt.show()


# TODO: move below to scratch file
# def tts(x, y, p):
#     """
#
#     :param x:
#     :param y:
#     :param p:
#     :return:
#     """
#     x_train, x_test, y_train, y_test = train_test_split(x, y, train_size=p)
#     return x_train, x_test, y_train, y_test


# # Not sure if i will use this or not
# def cleaning_repeating_char(tweet):
#     return re.sub(r'(.)\1+', r'\1', tweet)


# TODO this does .lower twice, but it lowers the handle things once
# def alternative_stem(tweet):
#     """Takes a tweet, returns the stemmed form of the tweet"""
#     words = [word if (word[0:2] == '__') else word.lower()
#              for word in tweet.split()
#              if len(word) >= 3]
#     words = [stemmer_.stem(w) for w in words]
#     return words


# debug
# twt = " is anybody going to the radio station tomorrow to see shawn? me and my friend may go but we would like to make new friends/meet there (:	"
# preprocess_tweets(twt)
