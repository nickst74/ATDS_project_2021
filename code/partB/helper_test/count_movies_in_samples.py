# runs locally just to count how many different movies exist in sample file (NOT on Apache Sark)
# only used to check that the join was executed correctly, as we took less than 100 deifferent movies
# at the result, and wanted to cross-validate it with the data
fp = open("/home/user/dataset/movie_genres_sample.csv", 'r')

movies = set()
for line in fp:
        tokens = line.split(',')
        m = tokens[0]
        if m not in movies:
                movies.add(m)

fp.close()
print("Number of movies in movie_genres_sample.csv is: " + str(len(movies)))
