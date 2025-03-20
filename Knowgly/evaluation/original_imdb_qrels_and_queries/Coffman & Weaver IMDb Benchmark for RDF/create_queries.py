import glob

query_files = glob.glob("*.txt")
output_file = open("queries_imdb.txt", "w")


for file in query_files:
    if file == "topics.txt" or file == "qrels_imdb.txt":
        continue
    print(file)
    results = []
    query_name = ""
    with open(file, 'r', encoding='latin-1') as f: # Some files have  encoding errors)
        lines = f.readlines()

        # Get the query name from the first line starting with '#'
        query_name = next(line.strip()[1:] for line in lines if line.startswith('#'))

        output_file.write(query_name.strip().replace(' ', '_') + "\t" + query_name + "\n")
