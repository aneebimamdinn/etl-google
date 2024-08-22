import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


def count_words(word):
    """Maps words to a key-value pair (word, 1)."""
    return (word, 1)


def format_result(word_count):
    """Formats the word count into a string."""
    word, count = word_count
    return f'{word}: {count}'


def run():
    options = PipelineOptions()
    with beam.Pipeline(options=options) as p:
        (p
         | "ReadFromFile" >> beam.io.ReadFromText("aneeb.txt")
         | "SplitInWordsLower" >> beam.FlatMap(break_into_words_lower)
         | "PairWithOne" >> beam.Map(count_words)
         | "CountAll" >> beam.CombinePerKey(sum)
         | "FormatResult" >> beam.Map(format_result)
         | "StoreInFile" >> beam.io.WriteToText("output-new.txt")
         )


def break_into_words_lower(content):
    words = content.lower().split()
    return words


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    run()

