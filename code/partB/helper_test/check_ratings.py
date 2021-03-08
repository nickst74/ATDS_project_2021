# runs locally just to count how many of the sample movies don't have a rating (NOT on Apache Sark)
# only used to check that the join was executed correctly, as we took less than 100 deifferent movies
# at the result, and wanted to cross-validate it with the data
fp = open("/home/user/dataset/ratings.csv", 'r')

movies = {"949","12110","1710","5","11517","17015","687","8844","33689","12665","9909","9091","8012","11860","9087","9273","11862","15602","9263","902","45325","451","9598","21032","78802","37557","1408","47018","862","4584","139405","16420","63","524","9691","31357","10858","710"}

for line in fp:
        tokens = line.split(',')
        m = tokens[1]
        if m in movies:
                movies.remove(m)

fp.close()
print("Movies with no ratings from sample.csv: "+str(len(movies)))
