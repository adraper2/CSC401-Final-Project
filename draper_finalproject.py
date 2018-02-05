#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 28 17:44:53 2017

@author: Draper
Final Project: Data collection
"""

# shows a user's playlists (need to be authenticated via oauth)
import spotipy
import spotipy.util as util
from math import ceil
from py_genius import Genius
import pymysql
import getpass
from urllib.request import Request, urlopen
from bs4 import BeautifulSoup
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from gensim import corpora
from gensim.models.ldamodel import LdaModel
from gensim.parsing.preprocessing import STOPWORDS

# set database connection parameters
dbhost = 'cs.elon.edu'
dbschema = 'adraper2'
dbuser = 'adraper2'
dbpasswd = getpass.getpass()
dbport = 3306
dbcharset = 'utf8mb4'

genius = Genius('4IM3f43SFRJAQ-26GAlWmrh2-xCn_OXdv7KQmA-gAL5CZdG0kdcUN5JHEkjGmEIW')

#parameters for client information on Spotify's API
scope = 'user-library-read user-top-read user-follow-read user-read-recently-played'
username = 'adraper19'

#attempts access to Spotify's API - post redirected URL in the console
token = util.prompt_for_user_token(username, scope,
                                   client_id='b5582a9933d5493d86879aca375ddb4f', 
                                   client_secret='8f28a368c39d42c7a971fa1ed1335a39',
                                   redirect_uri = 'http://localhost/')

#three insert queries for the respective databases
insertSavedSongs = 'INSERT INTO spotify_saved_songs(song, song_id, artist, artist_id, lyrics,\
                    valence, popularity, date_added)\
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s)'

insertGenre = 'INSERT INTO spotify_saved_genres(song_id, genre)\
               VALUES(%s, %s)'

insertTopics = 'INSERT INTO spotify_saved_topics(song_id, word1, word2, word3, word4, word5,\
                word6, word7, vader_words)\
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)'

#update instrumental songs to null values
updateNulls = 'UPDATE spotify_saved_songs SET lyrics = NULL\
               WHERE lyrics = "instrumental" or lyrics = ""'

#query to grab saved songs with lyrics
queryGrabSongs = 'SELECT * FROM spotify_saved_songs WHERE lyrics is not NULL'

#update queries for inserting vader score for song lyrics
updateSongVaders = 'UPDATE spotify_saved_songs SET vader_lyrics = %s\
                    WHERE saved_order = %s'

def main():
    collect()
    score()

#collect and insert data into a phpMyAdmin database
def collect():
    if token: #if access was granted
        #spotify API object 
        sp = spotipy.Spotify(auth=token)
        
        #grabs the total number of saved songs and then collects each bunch of 50 songs at a time because of the API's limit
        total = sp.current_user_saved_tracks(limit=50)['total']
        print('Expected Song Count:',total)
        savedSongs = [] #list of saved songs' json files
        for i in range(int(ceil(total/50.0))):
            offset = 50*i
            savedSongs.extend(sp.current_user_saved_tracks(limit=50, offset=offset)['items'])
        
        print('Song Count:', len(savedSongs)) #make sure these counts line up
        
        #loops through each songs provided json file info
        for songInfo in savedSongs:
            #grabs parameters to insert into the tables
            songName = songInfo['track']['name']
            songId = songInfo['track']['id']
            artistName = songInfo['track']['artists'][0]['name']
            artistId = songInfo['track']['artists'][0]['id']
            valence = sp.audio_features(songId)[0]['valence']
            pop = songInfo['track']['popularity']
            dateAdded = songInfo['added_at'][:songInfo['added_at'].find('T')]
            
            #attempt to grab song lyrics from genius using text scraping with BeautifulSoup
            try:
                #cleans name of featured artist
                cleanName = songName.split('(')[0]
                rList = genius.search(artistName + ' ' + cleanName) #searches genius on query artist + name
                # if the search results are not empty, see if the first one is a song, otherwise try the second 
                if rList:
                    rList = rList['response']['hits']
                    if rList[0]['type'] == 'song':
                        result = rList[0]['result']
                    elif rList[1]:
                        if rList[1]['type'] == 'song':
                            result = rList[1]['result']
                        else: #if the second is not a song, its probably not a match anyway 
                            lyrics = None
                            break 
                    else:
                        lyrics = None
                        break 
                #grabs title and url from the resultant song found if one is found
                title = result['full_title']
                gUrl = result['url']
                #safety check: is the grabbed search result song name equal to the spotify song name
                if title[:title.find('by')-1].lower().strip().strip('(').strip(')') in cleanName.lower().strip().strip('(').strip(')'):
                    #parameters for getting only the visible text
                    req = Request(gUrl,headers={'User-Agent': 'Mozilla/5.0'})
                    html = urlopen(req).read()
                    soup = BeautifulSoup(html, 'lxml')
                    #removes text under these tags
                    for s in soup(['style', 'script', '[document]', 'head', 'title']):
                        s.extract() 
                    visibleText = soup.getText() #gets only the visible text from the 'non-extracted' tags
                    lyrics = visibleText[visibleText.find('Lyrics')+6:visibleText.find('More on Genius')]
                    #cleans lyrics of '[Verse 1]', '[Chorus]', etc. and filters out blank lines
                    for x in range(lyrics.count(']')):
                        if lyrics.find(']') != -1:
                            lyrics = lyrics.replace(lyrics[lyrics.find('['):lyrics.find(']')+1],'')
                    lyrics = '\n'.join(list(filter(None, lyrics.split('\n')))) #filters out blank lines and joins list as string
                    print('YES')
                else: #if there is not a match, then no lyrics were found
                    lyrics = None
                    print('NO')
            except:
                print('GENIUS: NO LYRICS FOUND')
                lyrics = None
            
            cursor.execute(insertSavedSongs,(songName, songId, artistName, artistId, lyrics, valence, pop, dateAdded))

            #grabs the list of genres for the genres table
            genres = sp.artist(artistId)['genres']
            if genres: #if any genres were found, loop through the list and add them to the table with the song id
                for genre in genres:
                    cursor.execute(insertGenre,(songId, genre))

    
    else: #if access denied
        print("Can't get token for", username)

#score previously selected data - only run after running collect()
def score():

    cursor.execute(updateNulls) #safety check to catch bad text scraping
    sid = SentimentIntensityAnalyzer() #create sentiment analyzer object
    
    numTopics = 3     #how many topics would you like the model to find
    numWords = 7     #how many words would you like to view
    passes = 20        #how many times do you want to go over the data
    
    #a list of hardcoded words to ignore - created from analyzing previous trials
    stopWords = ['the', 'like', 'ya', 'wanna', 'know', 'let', 'got', '4', 'yeah', 'ooh', 'yo',
                 'went', 'ric', '2', 'need', 'seen', 'word', 'huh', 'said' 'big', 'whatchu',
                 'el', 'gonna', 'cause', 'things', 'gon', 'thing', 'letting', 'goes', 'tell', 
                 'gave', 'great', '10', 'uh', '25', 'said', 'stuff', 'tho', 'gotta', '100', 'al',
                 'lot', 'bout', 'boi', 'dem', 'oh', 'ooooahahh', '80', 'ig', 'ev', 'ayy', '85',
                 'vro', 'ok', 'ha', 'tings', 'nah', 'em', 'wit', 'mi', '6', '21', 'la', 'x2', 'ay',
                 'du', 'ba', 'im', 'ahhhh', '7', '12', 'yaaaaa', 'ee', 'waaaaaaa', 'mmm', 'na', 
                 'buh', 'ga', 'da', 'iii', '47', 'ol', 'une', '0', '1', '2015'] 
    
    cursor.execute(queryGrabSongs)
    data = cursor.fetchall() #grab all of the song data with lyrics
    print("Percent of total saved songs analyzed: ", str(round((len(data)/data[-1][0]),3)*100) + '%')
    
    #for each song with lyrics, individually score and update the vader column then grab the topics using an LDA model for each unique corpora
    for row in data:
        #safety check to make sure there are lyrics
        if row[5]:
            lyrics = row[5]
            rowId = row[0]
            #score the song lyrics grabbed and update the vader score column in the main table
            ss = sid.polarity_scores(lyrics)
            cursor.execute(updateSongVaders,(ss.get('compound'), rowId))
            
            lines = lyrics.split('\n') #create a list of lines in song lyrics for the bag of words
            
            #makes a list of the indiviudal words for the dict and corpus
            words = [[word.strip() for word in line.lower().split() if word 
                     not in STOPWORDS and word not in stopWords and word.isalnum()]
                     for line in lines]
            
            #dict and corpus from list of words
            dictionary = corpora.Dictionary(words)
            corpus = [dictionary.doc2bow(text) for text in words]
            
            try:
                #use lda on bag of words to find topics
                lda = LdaModel(corpus,
                               id2word=dictionary,
                               num_topics=numTopics,
                               passes=passes)
                
                #for each of the expected 3 topics, score the topic words as well as insert the topic words and vader score
                for topic in lda.print_topics(num_words = numWords):
                    listOfTerms = topic[1].split('+')
                    wordList = []
                    for term in listOfTerms:
                        listItems = term.split('*')
                        wordList.append(listItems[1].replace('"',''))
                    ss2 = sid.polarity_scores(' '.join(wordList))
                    cursor.execute(insertTopics, (row[2],wordList[0],wordList[1],wordList[2],wordList[3],wordList[4],
                                                  wordList[5], wordList[6], ss2.get('compound')))
            except: #safety check to make sure techno and EDM songs with overly repetitve lyrics are not added
                print('Bag of Words too small')
                
    print('-----DONE!-----')
            
    print(len(data),'Scores and Topics Added')
    
# Open local database connection
db = pymysql.connect(host=dbhost,
                     db=dbschema,
                     user=dbuser,
                     passwd=dbpasswd,
                     port=dbport,
                     charset=dbcharset,
                     autocommit=True)


cursor = db.cursor() #creates cursor object
main() #runs program
db.close() #closes connection