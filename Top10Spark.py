
import time
#pega os nomes dos filmes e coloca chaves com o mesmo numero do token
def load_movies(filename):
    m_names ={}
    with open(filename, encoding = 'ISO-8859-1') as a:
        for line in a:
            tokens = line.split('::')
            key=int(tokens[0])
            m_names[key]=tokens[1]
    return m_names

#pegar avaliações
lines =sc.textFile('C:/Users/andre/Documents/GitHub/Top10Movies/Dados/ratings.dat')

#importar o nome dos filmes
m_names =load_movies('C:/Users/andre/Documents/GitHub/Top10Movies/Dados/movies.dat')

#associar avaliação com o nome do filme
m_rating=lines.map(lambda x:(int(x.split('::')[1]),[float(x.split('::')[2]),1]))

#somatório de todas as notas dos filmes
rating_count = m_rating.reduceByKey(lambda x, y: [x[0] + y[0], x[1] + y[1]])

#fazer média das notas
def media(x): return x[0]/x[1]

#colocar medias na lista
total = rating_count.mapValues(media)

flipped = total.map(lambda y: (y[1], y[0]))

#organizar por nota
sort = flipped.sortBykey(False)

#pega os 10 primeiros da lista
results = sort.take(10)

#imprime os 10 mais avaliados a cada 15 segundos
while True:  
    results=sort.take(10)
    for result in results:
        key=result[1]
        value=result[0]
        print(m_names[key],"--", round(value,2))  
    print('----')
    time.sleep(15)