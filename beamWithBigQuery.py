import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
import uuid


def count_words(word):
    """Maps words to a key-value pair (word, 1)."""
    return (word, 1)


def format_result(word_count):
    """Formats the word count into a string."""
    word, count = word_count
    return {'id': str(uuid.uuid4()), 'word': word, 'count': count}


def run():
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        (p
         | "ReadFromFile" >> beam.io.ReadFromText("aneeb.txt")
         | "SplitInWordsLower" >> beam.FlatMap(break_into_words_lower)
         | "PairWithOne" >> beam.Map(count_words)
         | "CountAll" >> beam.CombinePerKey(sum)
         | "FormatResult" >> beam.Map(format_result)
         | "WriteToBigQuery" >> WriteToBigQuery(
            table='<project>:<dataset>.words_count',
            schema='id:STRING, word:STRING, count:INTEGER',
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            custom_gcs_temp_location="gs://dataflow-apache-22396/temp"
            )
         )


def break_into_words_lower(content):
    words = content.lower().split()
    return words


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    run()

