import re
import csv
import sys
import time
import nltk
import pandas as pd
import apache_beam as beam
from collections import Counter
from google.cloud import storage
from nltk.corpus import stopwords
from urllib.request import urlopen
from collections import defaultdict
from apache_beam.io import WriteToText
from apache_beam.io import ReadFromText
from nltk.tokenize import word_tokenize
from apache_beam.pipeline import StandardOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions

nltk.download('stopwords')
nltk.download('punkt')

options = PipelineOptions()
google_cloud_options = options.view_as(GoogleCloudOptions)
google_cloud_options.job_name = 'edgar'
google_cloud_options.project = 'zinc-forge-265718'
google_cloud_options.staging_location = 'gs://7245-pipeline/staging'
google_cloud_options.temp_location = 'gs://7245-pipeline/temp'
options.view_as(StandardOptions).runner = 'DataflowRunner'

p = beam.Pipeline(options=options)


class Split(beam.DoFn):
    def process(self, element):
        Company, Year, Filing = element.split(",")
        return [{
            'Company': str(Company),
            'Year': str(Year),
            'Filing': str(Filing),
        }]


class Attach(beam.DoFn):

    def process(self, element):

        from urllib.request import urlopen

        url = 'https://www.sec.gov/Archives/edgar/full-index/%s/QTR1/master.idx' % (
            element['Year'])

        response = urlopen(url)

        string_match1 = 'edgar/data/'
        element2 = 'NA'
        element3 = 'NA'
        element4 = 'NA'

        for lin in response:
            line = lin.decode()
            if element['Company'] in line and element['Filing'] in line:
                for e in line.split(' '):
                    if string_match1 in e:
                        element2 = e.split('|')
                        for element3 in element2:
                            if string_match1 in element3:
                                element4 = element3

        url3 = 'https://www.sec.gov/Archives/' + element4

        element.update([('Link', url3)])
        element['Link'] = element['Link'].rstrip()

        return [{
            'Company': element['Company'],
            'Year': element['Year'],
            'Filing': element['Filing'],
            'Link': element['Link'],
        }]


class scrapemeta(beam.DoFn):


    def process(self, element):
        import logging
        import csv
        import urllib3
        import requests
        from collections import Counter        
        CIK = element['cik'] #CIK
        logging.info(CIK)
        Year = element['year'] #YEAR 
        logging.debug(Year)
        FILE=element['filing']
        for x in range(1,5):
            url='https://www.sec.gov/Archives/edgar/full-index/%s/QTR'%(Year) + str(x) + '/master.idx'
            response = requests.get(url)
            logging.info(response)
            string_match1 = 'edgar/data/' 
            element2 = None
            element3 = None
            element4 = None
            for line in response.text.splitlines():
                if CIK in line and FILE in line:
                    #print("working")iii
                    for element in line.split(' '):
                        if string_match1 in element:
                            element2=element.split('|') 
                            for element3 in element2:
                                if string_match1 in element3:
                                    element4=element3
                                    return [{'cik':CIK, 'year':Year , 'file':FILE ,'link':'https://www.sec.gov/Archives/' +element4}]
      
class Splitmeta(beam.DoFn):
    def process(self, element):
        cik,year,filing = element.split(',')
        return [{
            'cik': cik,
            'year': year,
            'filing':filing
        }]



class Preprocess(beam.DoFn):

    def process(self, element):

        import nltk
        from nltk.corpus import stopwords
        from nltk.tokenize import word_tokenize
        from urllib.request import urlopen
        nltk.download('stopwords')
        nltk.download('punkt')

        str_response = urlopen(element['Link']).read().decode('utf-8')
        stop_words = set(stopwords.words('english'))
        word_tokens = word_tokenize(str_response)

        filtered_sentence = [w for w in word_tokens if not w in stop_words]
        filtered_sentence = []
        for w in word_tokens:
            if w not in stop_words:
                filtered_sentence.append(w)

        fresponse = [word for word in filtered_sentence if word.isalpha()]

        element.update([('WordList', fresponse)])

        return [{
            'Company': element['Company'],
            'Year': element['Year'],
            'Filing': element['Filing'],
            'Link': element['Link'],
            'WordList': element['WordList'],
        }]



class NLTKTokenizer(beam.DoFn):
    def process(self, element):
        import nltk
        from nltk.corpus import stopwords
        from nltk.tokenize import word_tokenize
        fdist = nltk.FreqDist(element['WordList'])

        words = []
        frequencies = []

        for word, frequency in fdist.most_common(1000000):
            words.append(word)
            frequencies.append(frequency)

        element.update([('WordList', words)])
        element.update([('Frequency', frequencies)])

        return [{
            'Company': element['Company'],
            'Year': element['Year'],
            'Filing': element['Filing'],
            'Link': element['Link'],
            'WordList': element['WordList'],
            'Frequency': element['Frequency'],
        }]



class ProcessWords(beam.DoFn):

    def process(self, element, filep, filen, fileu, filel, files, filew, filec):

        import csv
        import apache_beam as beam
        from apache_beam.options.pipeline_options import PipelineOptions
        from apache_beam.io import ReadFromText
        from apache_beam.io import WriteToText
        from urllib.request import urlopen
        import time
        import csv
        import sys
        from collections import defaultdict
        import pandas as pd
        import re
        import nltk
        from nltk.corpus import stopwords
        from nltk.tokenize import word_tokenize

        company = ()
        year = ()
        filing = ()
        link = ()
        wordlist = ()
        frequency = ()
        wordtype = ()

        positive = filep
        negative = filen
        uncertain = fileu
        ligitious = filel
        strongmodal = files
        weakmodal = filew
        constraining = filec

        for i in range(0, len(element['WordList'])):
            for word in positive:
                if word.lower() == (element['WordList'][i]).lower():
                    company += (element['Company'],)
                    year += (element['Year'],)
                    filing += (element['Filing'],)
                    link += (element['Link'],)
                    wordlist += (element['WordList'][i],)
                    frequency += (element['Frequency'][i],)
                    wordtype += ('Positive',)
            for word in negative:
                if word.lower() == (element['WordList'][i]).lower():
                    company += (element['Company'],)
                    year += (element['Year'],)
                    filing += (element['Filing'],)
                    link += (element['Link'],)
                    wordlist += (element['WordList'][i],)
                    frequency += (element['Frequency'][i],)
                    wordtype += ('Negative',)
            for word in uncertain:
                if word.lower() == (element['WordList'][i]).lower():
                    company += (element['Company'],)
                    year += (element['Year'],)
                    filing += (element['Filing'],)
                    link += (element['Link'],)
                    wordlist += (element['WordList'][i],)
                    frequency += (element['Frequency'][i],)
                    wordtype += ('Uncertainity',)
            for word in ligitious:
                if word.lower() == (element['WordList'][i]).lower():
                    company += (element['Company'],)
                    year += (element['Year'],)
                    filing += (element['Filing'],)
                    link += (element['Link'],)
                    wordlist += (element['WordList'][i],)
                    frequency += (element['Frequency'][i],)
                    wordtype += ('Litigious',)
            for word in strongmodal:
                if word.lower() == (element['WordList'][i]).lower():
                    company += (element['Company'],)
                    year += (element['Year'],)
                    filing += (element['Filing'],)
                    link += (element['Link'],)
                    wordlist += (element['WordList'][i],)
                    frequency += (element['Frequency'][i],)
                    wordtype += ('Strong Modal',)
            for word in weakmodal:
                if word.lower() == (element['WordList'][i]).lower():
                    company += (element['Company'],)
                    year += (element['Year'],)
                    filing += (element['Filing'],)
                    link += (element['Link'],)
                    wordlist += (element['WordList'][i],)
                    frequency += (element['Frequency'][i],)
                    wordtype += ('Weak Modal',)
            for word in constraining:
                if word.lower() == (element['WordList'][i]).lower():
                    company += (element['Company'],)
                    year += (element['Year'],)
                    filing += (element['Filing'],)
                    link += (element['Link'],)
                    wordlist += (element['WordList'][i],)
                    frequency += (element['Frequency'][i],)
                    wordtype += ('Constraining',)            

        # Appending words,frequencies and word type to output dictionaries
        element.update([('Company', company)])
        element.update([('Year', year)])
        element.update([('Filing', filing)])
        element.update([('Link', link)])
        element.update([('Word', wordlist)])
        element.update([('Frequency', frequency)])
        element.update([('WordType', wordtype)])

        return[{
                'Company': element['Company'],
                'Year': element['Year'],
                'Filing': element['Filing'],
                'Link': element['Link'],
                'Word': element['Word'],
                'WordType': element['WordType'],
                'Frequency': element['Frequency'],
            }]


class WriteToCSV(beam.DoFn):
    def process(self, element):

        result = []
        for i in range(0, len(element['Word'])):
            result.append("{},{},{},{},{},{}".format(
                element['Company'][i], element['Year'][i], element['Filing'][i], element['Word'][i], element['WordType'][i], element['Frequency'][i]))

        return result


    
datap = (p|'Positive' >> beam.io.ReadFromText('gs://7245-pipeline/words/positive-test.csv'))        
datan = (p|'Negative' >> beam.io.ReadFromText('gs://7245-pipeline/words/negative-test.csv'))
datau = (p|'Uncertain' >> beam.io.ReadFromText('gs://7245-pipeline/words/uncertainity-test.csv'))
datal = (p|'Litigious' >> beam.io.ReadFromText('gs://7245-pipeline/words/litigious-test.csv'))
datas = (p|'Strongmodal' >> beam.io.ReadFromText('gs://7245-pipeline/words/strongmodal-test.csv'))
dataw = (p|'Weakmodal' >> beam.io.ReadFromText('gs://7245-pipeline/words/weakmodal-test.csv'))
datac = (p|'Constraining' >> beam.io.ReadFromText('gs://7245-pipeline/words/constraining-test.csv'))
datafile = (p|'Reading input CSV' >> beam.io.ReadFromText('gs://7245-pipeline/file.csv'))


data_from_source = (datafile    | 'Split CIK' >> beam.ParDo(Split())
                                | 'Make URL' >> beam.ParDo(Attach()))

processing = (data_from_source  | 'Get filing' >> beam.ParDo(Preprocess())
                                | 'Tokenize (NLTK)' >> beam.ParDo(NLTKTokenizer())
                                | 'Compare words' >> beam.ParDo(ProcessWords(), beam.pvalue.AsList(datap), beam.pvalue.AsList(datan), beam.pvalue.AsList(datau),beam.pvalue.AsList(datal), beam.pvalue.AsList(datas), beam.pvalue.AsList(dataw), beam.pvalue.AsList(datac))
                                | 'Convert to CSV' >> beam.ParDo(WriteToCSV())
                                | 'Save to bucket' >> beam.io.WriteToText('gs://7245-pipeline/CIK/YEAR/FILING/output.csv'))


metadata =  (datafile           | 'Read input' >> beam.ParDo(Splitmeta()) 
                                | 'Scrape meta' >> beam.ParDo(scrapemeta()) 
                                | 'Writing To File' >> beam.io.WriteToText('gs://7245-pipeline/CIK/YEAR/FILING/metadata.csv'))


result = p.run().wait_until_finish()
