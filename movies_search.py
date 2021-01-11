import pandas as pd
from sqlalchemy import create_engine
import pymysql.cursors
import config

connection = pymysql.connect(host = '127.0.0.1',
                             user = config.user,
                             password = config.password,
                             db = 'bdd_ecoquelet')

server = "127.0.0.1" # : Si connection SSH avec tunnel port 3306 à partir d'internet
# datalab-mame.myconnectech.fr : Si sur réseau local
# server = "datalab-mame.myconnectech.fr" 
# BDname="grp_movies3"
BDname="bdd_ecoquelet"

cnx = create_engine('mysql+pymysql://' + config.user + ':' + config.password + '@' + server + '/' + BDname).connect()

print("Recherche dans la BDD")
search = input("On cherche quoi?\nFilms par titre (Titre)\nFilms par l'année de sortie (Année)\nFilms par Type (Type)\nFilms par genre (Genre)\nRecherche un acteur ou un producteur (Nom)\nTrouver un nom oublié (Je sais plus)\nTitre par les mots clés (Je tente)\nFilms par les notes (Note)\nLes series (Serie)\n")

if search == 'Titre':
    #SEARCH BY TITLE
    title = input("Quel film?")
    title = title.replace(' ', '')
    sql = 'SELECT * FROM title_basics AS B JOIN title_ratings AS R ON (B.tconst=R.tconst) WHERE primaryTitle = "' + title + '" OR originalTitle = "'+ title +'" ORDER BY titleType;'
    df = pd.read_sql(sql, cnx)
    print(df)

elif search == 'Année':
    #RECHERCHE PAR ANNEE
    minYear = input("Le film doit pas être plus vieux que...")
    minYear = minYear.replace(' ', '')
    maxYear = input("Et pas plus recent que...")
    maxYear = maxYear.replace(' ', '')
    sql = 'SELECT * FROM title_basics JOIN title_ratings ON (title_basics.tconst=title_ratings.tconst) WHERE (startYear >= "'+ minYear +'" AND startYear >= "'+ maxYear +'" ) ORDER BY startYear LIMIT 50;'
    df = pd.read_sql(sql, cnx)
    print(df)

elif search =='Type':
    #SEARCH BY TYPE
    titletype = input("Quelle type d'oeuvre? (\nshort\nmovie\ntvShort\ntvMovies\ntvSeries\ntvEpisode\ntvMiniSeries\ntvSpecial\nvideo\nvideoGame\n)")
    titletype = titletype.replace(' ', '')
    sql = 'SELECT * FROM title_basics AS B JOIN title_ratings AS R ON (B.tconst=R.tconst) WHERE titleType = "' + titletype + '" ORDER BY primaryTitle LIMIT 20;'
    df = pd.read_sql(sql, cnx)
    print(df)

elif search == 'Genre':
    #SEARCH BY GENRE
    genre = input("Quelle genre? (Documentary, Animation, Comedy, Short, Romance, Action, News\nDrama, Fantasy, Horror, Biography, Music, Crime, Familly\nAdventure, History, Mystery, Musical, War, Sci-Fi, Western\nThriller, Sport, Film-Noir, Talk-Show, Game-Show, Adult, Reality-TV\n)")
    sql = 'SELECT title_basics.titleType, title_basics.primaryTitle, title_basics.startYear, title_basics.runtimeMinutes, TBView.tconst, TBView.genre1, TBView.genre2, TBView.genre3, title_ratings.tconst, title_ratings.averageRating, title_ratings.numVotes FROM title_basics JOIN TBView JOIN title_ratings ON (title_basics.tconst=TBView.tconst) AND (title_basics.tconst=title_ratings.tconst) WHERE (genre1 = \'Comedy\' OR genre2 = \'Comedy\' OR genre3 = \'Comedy\') ORDER BY averageRating DESC LIMIT 50;'
    df = pd.read_sql(sql, cnx)
    print(df)
    
elif search == 'Nom':
    #RECHERCHE PERSONNE
    name = input("Le nom recherché?\n")
    sql = 'SELECT NBView.birthYear, NBView.deathYear, NBView.primaryName, NBView.PrimeProf1, title_basics.titleType, title_basics.primaryTitle, title_basics.startYear FROM NBView JOIN title_basics ON (NBView.KnownFor1=title_basics.tconst) OR (NBView.KnownFor2=title_basics.tconst) OR (NBView.KnownFor3=title_basics.tconst) OR (NBView.KnownFor4=title_basics.tconst) WHERE primaryName = "'+ name +'" ORDER BY startYear';
    df = pd.read_sql(sql, cnx)
    print(df)
    
elif search == 'Je sais plus':
    name = input('Nom approximatif?...:\n')
    try:
        with connection.cursor() as cursor:
            sql = "SET @name='%" + name + "%';";
            cursor.execute(sql)
            cursor.execute("SELECT * FROM name_basics WHERE primaryName LIKE @name;")
            results = cursor.fetchall()
            for result in results:
                print("{:10s} : {}".format(result[0], result[1]))
    finally:
        connection.close()

elif search == 'Je tente':
    #RECHERCHE PAR LES MOT CLES DANS LE TITRE 
    titre = input("Un ou des mot.s clé.s d'un titre de film?\n")
    sql = 'SELECT primaryTitle, originalTitle, startYear, genres, MATCH (primaryTitle) AGAINST ("'+ titre +'") AS score FROM title_basics WHERE MATCH (primaryTitle) AGAINST ("'+ titre +'") LIMIT 20';
    df = pd.read_sql(sql, cnx)
    print(df)
    
elif search == 'Note':
    #RECHERCHE PAR VOTES
    noteMin = input("Note minimale\n")
    noteMax = input("Note maximale?\n")
    nbVotes = input("Nombre des votes?\n")
    sql = 'SELECT * FROM title_basics JOIN title_ratings ON (title_basics.tconst=title_ratings.tconst) WHERE (averageRating >= "' + noteMin + '" AND averageRating <= "'+ noteMax +'" AND numVotes >= "'+ nbVotes +'") ORDER BY averageRating DESC LIMIT 30;'
    df = pd.read_sql(sql, cnx)
    print(df)

elif search == 'Serie':
    #RECHERCHE SERIE
    serie = input("On les classe comment?\nNombre de saisons (NS)\nNomre d'episodes (NE)\nAnnée (A)\nDurée en minutes (D)\nGenre (G)\n...Ou les mots clées dans le titre? (MC)\n")
    if serie == 'NS':
        sql = 'SELECT title_episode.seasonNumber, title_episode.episodeNumber, title_basics.primaryTitle, title_basics.originalTitle, title_basics.startYear, title_basics.endYear, title_basics.runtimeMinutes, title_basics.genres FROM title_episode JOIN title_basics ON (title_episode.tconst=title_basics.tconst) ORDER BY seasonNumber DESC LIMIT 50;';
        df = pd.read_sql(sql, cnx)
        print(df)
    elif serie == 'NE':
        sql = 'SELECT title_episode.seasonNumber, title_episode.episodeNumber, title_basics.primaryTitle, title_basics.originalTitle, title_basics.startYear, title_basics.runtimeMinutes, title_basics.genres FROM title_episode JOIN title_basics ON (title_episode.tconst=title_basics.tconst) ORDER BY episodenumber DESC LIMIT 50;';
        df = pd.read_sql(sql, cnx)
        print(df)
    elif serie == 'A':
        sql = 'SELECT title_episode.seasonNumber, title_episode.episodeNumber, title_basics.primaryTitle, title_basics.originalTitle, title_basics.startYear, title_basics.runtimeMinutes, title_basics.genres FROM title_episode JOIN title_basics ON (title_episode.tconst=title_basics.tconst) ORDER BY startYear DESC LIMIT 50;';
        df = pd.read_sql(sql, cnx)
        print(df)
    elif serie == 'D':
        sql = 'SELECT title_episode.seasonNumber, title_episode.episodeNumber, title_basics.primaryTitle, title_basics.originalTitle, title_basics.startYear, title_basics.runtimeMinutes, title_basics.genres FROM title_episode JOIN title_basics ON (title_episode.tconst=title_basics.tconst) ORDER BY runtimeMinutes DESC LIMIT 50;';
        df = pd.read_sql(sql, cnx)
        print(df)
    elif serie == 'G':
        sql = 'SELECT title_episode.seasonNumber, title_episode.episodeNumber, title_basics.primaryTitle, title_basics.originalTitle, title_basics.startYear, title_basics.runtimeMinutes, title_basics.genres FROM title_episode JOIN title_basics ON (title_episode.tconst=title_basics.tconst) ORDER BY genre DESC LIMIT 50;';
        df = pd.read_sql(sql, cnx)
        print(df)
    elif serie == 'MC':
        mc = input("Un ou des mot.s clé.s d'un titre de film?\n")
        sql = 'SELECT primaryTitle, originalTitle, startYear, genres, MATCH (primaryTitle) AGAINST ("'+ mc +'") AS score FROM title_basics WHERE MATCH (primaryTitle) AGAINST ("'+ mc +'") LIMIT 20';
        df = pd.read_sql(sql, cnx)
        print(df)
