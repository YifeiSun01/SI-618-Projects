import mrjob
import json
import csv
from mrjob.job import MRJob
from mrjob.step import MRStep

class RevenueCount(MRJob):
    OUTPUT_PROTOCOL = mrjob.protocol.TextProtocol
    def mapper(self, _, line):
        try:
            # line = line.encode(encoding = 'UTF-8', errors = 'ignore')
            line_list = list(csv.reader([line], delimiter=',', quotechar='"'))[0]
            genres = line_list[3]
            id =  line_list[5]
            original_language = line_list[7]
            title = line_list[20]
            release_date = line_list[14]
            spoken_languages = line_list[17]
            release_year = release_date[0:4]
            revenue = int(line_list[15])
            if genres != "":
                genres_json_list = json.loads(genres.replace("'", '"'))
                for dictionary in genres_json_list:
                    genre = dictionary["name"]
                    yield (release_year,"Genre:",genre),(1,revenue)
            if spoken_languages != "":
                spoken_languages_json_list = json.loads(spoken_languages.replace("'", '"'))
                dictionary = spoken_languages_json_list[0]
                language = dictionary["name"]
                yield (release_year,"Language:",language),(1,revenue)
            
        except:
            pass
    def combiner(self, key, values):
        counts = 0
        revenues = 0
        for value in values:
            counts += value[0]
            revenues += value[1]
        yield key, (counts,revenues)
    def reducer(self, key, values):
        counts = 0
        revenues = 0
        for value in values:
            counts += value[0]
            revenues += value[1]
        yield key, (counts,revenues)



    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                combiner = self.combiner,
                reducer = self.reducer),
            MRStep(reducer = self.reducer_find_max_word_stem)
        ]



if __name__ == '__main__':
    RevenueCount.run()