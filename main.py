# This is a sample Python script.

# Press ⌃R to execute it or replace it with your code.
# Press Double ⇧ to search everywhere for classes, files, tool windows, actions, and settings.


def run(name):
    #Read from text file
    with open('aneeb.txt', 'r') as file:
        content = file.read()

    #Get the words from the content
    words = break_into_words_lower(content)

    #now create wordwise count
    count = count_words(words)
    return count


def break_into_words_lower(content):
    words = content.lower().split()
    return words

def count_words(words):
    counter = {}
    for word in words:
        if word in counter:
            counter[word] += 1
        else:
            counter[word] = 1
    return counter


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    run('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
