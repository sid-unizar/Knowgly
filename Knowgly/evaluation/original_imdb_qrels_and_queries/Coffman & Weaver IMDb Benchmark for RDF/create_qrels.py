import glob

query_files = glob.glob("*.txt")
output_file = open("qrels_imdb.txt", "w")


for file in query_files:
    if file == "topics.txt" or file == "qrels_imdb.txt":
        continue
    print(file)
    results = set()
    query_name = ""
    with open(file, 'r', encoding='latin-1') as f: # Some files have encoding errors)
        lines = f.readlines()

        # Get the query name from the first line starting with '#'
        query_name = next(line.strip()[1:] for line in lines if line.startswith('#'))

        # Get the results enclosed by '<' and '>', ignoring comments to the right
        for line in lines:
            if '<' in line:
                result_line = line.split('#', 1)[0].strip()
                for r in [result.strip() for result in result_line.split(',')]:
                    results.add(r)

        #print("Query Name:", query_name)
        #print("Results:", results)
        #print()


    for result in results:
        output_file.write(query_name.strip().replace(' ', '_') + " Q0 " + result + " 1\n")
