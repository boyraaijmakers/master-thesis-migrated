
import storm

import spacy
import en_core_web_sm

class SpacyNERBolt(storm.BasicBolt):

	nlp = en_core_web_sm.load()

	def process(self, tup):
		article = nlp(tup.values[0])
		orgs = [X.text for X in article.ents if X.label_ == "ORG"]
		storm.emit([max(set(orgs), key = orgs.count), tup.values[2], tup.values[3]])

SpacyNERBolt().run()