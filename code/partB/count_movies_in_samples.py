fp = open("/home/user/dataset/movie_genres_sample.csv", 'r')

movies = set()
for line in fp:
        tokens = line.split(',')
        m = tokens[0]
        if m not in movies:
                movies.add(m)

fp.close()
print("Number of movies in movie_genres_sample.csv is: " + str(len(movies)))
