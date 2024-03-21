import os

# Define the directory containing the CSV files
directory = './data/train/'

# Iterate over all CSV files in the directory
for filename in os.listdir(directory + 'old_data/'):
    if filename.endswith(".csv"):
        # Define the path to the original CSV file and the path to the preprocessed CSV file
        original_file = os.path.join(directory + 'old_data/', filename)
        preprocessed_file = os.path.join(directory, f"preprocessed_{filename}")

        # Read the original CSV file line by line, preprocess each line, and write to the preprocessed file
        with open(original_file, 'r', encoding='utf-8') as input_file:
            with open(preprocessed_file, 'w', encoding='utf-8') as output_file:
                for line in input_file:
                    # Remove backslashes from the line
                    preprocessed_line = line.replace('\\""', '')
                    output_file.write(preprocessed_line)
