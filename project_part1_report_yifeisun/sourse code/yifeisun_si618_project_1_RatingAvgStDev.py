import mrjob
import json
import csv
from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingAvgStDev(MRJob):
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
            vote_average = float(line_list[-2])
            if genres != "":
                genres_json_list = json.loads(genres.replace("'", '"'))
                for dictionary in genres_json_list:
                    genre = dictionary["name"]
                    yield (release_year,"Genre:",genre),(1,vote_average,vote_average**2)
            if spoken_languages != "":
                spoken_languages_json_list = json.loads(spoken_languages.replace("'", '"'))
                dictionary = spoken_languages_json_list[0]
                language = dictionary["name"]
                yield (release_year,"Language:",language),(1,vote_average,vote_average**2)
            yield (release_year,"All Languages All",""),(1,vote_average,vote_average**2)
        except:
            pass
    def combiner(self, key, values):
        counts = 0
        ratings = 0
        rating_sqs = 0
        for value in values:
            counts += value[0]
            ratings += value[1]
            rating_sqs += value[2]
        yield key, (counts,ratings,rating_sqs)
    def reducer(self, key, values):
        counts = 0
        ratings = 0
        rating_sqs = 0
        for value in values:
            counts += value[0]
            ratings += value[1]
            rating_sqs += value[2]
        yield key, (counts,ratings,rating_sqs)
    def st_dev_calculator(self, key, values):
        for value in values:
            n = value[0]
            if n >= 10:
                x_sum = value[1]
                x2_sum = value[2]
                avg = x_sum/n
                st_dev = ((x2_sum-(x_sum**2/n))/(n-1))**(1/2)
                yield key[0]+"\t\t"+key[1]+"\t"+key[2], "Mean:\t"+str(round(avg,2))+"\t\tStandard Deviation:\t"+str(round(st_dev,2))

    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                combiner = self.combiner,
                reducer = self.reducer),
            MRStep(reducer = self.st_dev_calculator)
        ]

if __name__ == '__main__':
    RatingAvgStDev.run()