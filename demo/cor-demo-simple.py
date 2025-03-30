import re

path_to_input_text = "cor-demo-sample.txt" # Replace this by your testing input data.

def drop_empty_item(words):
    return [word for word in words if word != '']

def sort_dict_by_keys(d, reverse=True):
    keys = list(d.keys())
    keys.sort(reverse=reverse)
    return [(key,d[key]) for key in keys]

def count_items(items):
    d = {}
    for item in items:
        d[item] = d.get(item, 0) + 1
    return d

# Step 1: Tokenize the input text.
paragraph = ""
lines = []

with open(path_to_input_text, "r") as f:
    for line in f.readlines():
        lines.append(drop_empty_item(re.sub("[^a-zA-Z]", " ", line).split(" ")))
        paragraph += line

paragraph = re.sub("[^a-zA-Z]", " ", paragraph)

words = paragraph.split(" ")
words = drop_empty_item(words)
word_pairs = []

# Step 2: Count the frequency for each word in the text (you should complete this in your first-pass MapReduce).
word_counts = count_items(words)
word_counts = sort_dict_by_keys(word_counts, False)
single_key_freq = {}
for tmp in word_counts:
    word = tmp[0]
    cnt = tmp[1]
    single_key_freq[word] = float(cnt)

# Step 3: Count the frequency for each word pairs in the text (you should complete this in your second-pass MapReduce).
for line in lines:
    line_counts = count_items(line)
    line_counts = sort_dict_by_keys(line_counts, False)
    words = [y[0] for y in line_counts]
    for id1 in range(len(words)):
        for id2 in range(id1+1, len(words)):
            word_pairs.append(words[id1]+"/"+words[id2])

# Step 4: Calculate the COR(A, B) for each word pairs in the text (you should complete this in your second-pass MapReduce).
pair_counts = count_items(word_pairs)
results = []
for pair in pair_counts:
    cnt = pair_counts[pair]
    pair_freq = float(cnt)
    words = pair.split("/")
    COR = pair_freq / (single_key_freq[words[0]] * single_key_freq[words[1]])
    results.append((words[0], words[1], COR))

# Sort results by word1 and word2
results.sort(key=lambda x: (x[0], x[1]))

# Write results to file
with open("result.csv", "w") as f:
    for word1, word2, cor in results:
        f.write("%s\t%s\t%s\n" % (word1, word2, cor)) 