from pyspark import SparkContext
import string
import os

def normalize_words(words):
    """
    list of strings -> list of strings
    Return cleaned words from a list of words.
    """
    try:
        words = words.encode('utf-8')
    except:
        print("The file is already in utf-8 format")
    words = words.decode('utf-8')
    words = words.lower()
    "Remove punctuation from strings"
    normalized_words = words.translate(str.maketrans('', '', string.punctuation))
    return normalized_words


def input_file(path, filename, sc, rddList):
    """
    string, string, spark_content, list of RDD -> None
    Convert files in a path to rdds and append the rdds into a list.
    """
    text_file = sc.textFile(path + filename)
    words_rdd = text_file.flatMap(lambda line: normalize_words(line).split()) \
        .filter(lambda x: len(x) > 0)
    words_rdd = words_rdd.map(lambda x: (x, filename)).distinct()

    rddList.append(words_rdd)

def main(input_folder, output_folder):
    """
     build an inverted index of the documents to speed up calculation.
    """
    sc = SparkContext.getOrCreate()
    rddList = []

    # Read text files
    for files in os.listdir(input_folder):
        # Clean File
        try:
            input_file(input_folder, files, sc, rddList)
        except:
            print("The filename must be integers.")

    # Combine RDD into 1
    try:
        combined_rdds = sc.union(rddList) \
            .map(lambda x: (x[0], [int(x[1])])) \
            .reduceByKey(lambda x, y: x + y) \
            .map(lambda x: (x[0], sorted(x[1])))
    except:
        print("There are no files in input folder")

    # Create Index for dictionary
    word_dict = combined_rdds.map(lambda x: x[0]).zipWithIndex()
    word_dict.saveAsTextFile(output_folder + 'dictionary')

    # Convert to index values
    word_dict = word_dict.collectAsMap()
    combined_rdds = combined_rdds.map(lambda x: (word_dict[x[0]], x[1]))

    # Output Files
    combined_rdds.saveAsTextFile(output_folder + 'reverse_index')

main('../input/', '../output/')



