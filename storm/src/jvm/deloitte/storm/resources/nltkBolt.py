import storm
import re, string, unicodedata
import inflect
from nltk.corpus import stopwords

class NLTKBolt(storm.BasicBolt):

    def process(self, tup):
        p = inflect.engine()

        body = tup.values[0]

        out_sentences = []
        for sentence in body.split(". "):
            words = sentence.split(" ")
            new_words = []
            for word in words:
                # Remove none ASCII characters
                new_word = unicodedata.normalize('NFKD', word).encode('ascii', 'ignore').decode('utf-8', 'ignore')

                # Remove punctuation
                new_word = re.sub(r'[^\w\s]', '', new_word)

                # Convert numerical number to textual number (6 -> six)
                if new_word.isdigit():
                    new_word = p.number_to_words(new_word)

                # Remove stopwords
                if new_word in stopwords.words('english'):
                    new_word = ''

                if new_word != '':
                    new_words.append(new_word)

            out_sentences += [" ".join(new_words) + "."]

        storm.emit([" ".join(out_sentences), tup.values[1], tup.values[2], tup.values[3]])

NLTKBolt().run()