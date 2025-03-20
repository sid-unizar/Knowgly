import sys


input_file = sys.argv[1]

try:
    with open(input_file, 'r') as file:
        for line in file:
            columns = line.strip().split()
            if len(columns) >= 6:
                del columns[1]
                del columns[4]
                del columns[3]
                print('\t'.join(columns))
            else:
                print("Invalid line:", line)

except FileNotFoundError:
    print(f"File '{input_file}' not found.")
    sys.exit(1) 
