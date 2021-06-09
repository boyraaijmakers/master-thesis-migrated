
import storm
from textblob import TextBlob

class NLTKBolt(storm.BasicBolt):

    def process(self, tup):
        body = tup.values[0]
        blob = TextBlob(body)
        pol = 1 if blob.sentiment.polarity >= 0 else -1

        storm.emit([str(pol), tup.values[1], tup.values[3]])

NLTKBolt().run()