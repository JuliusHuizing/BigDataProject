import pandas as pd
import numpy as np
import json 
import re
import html
import gensim.downloader as api
from unidecode import unidecode

from deep_translator import GoogleTranslator
from time import time

class DataTranslator:

    def __init__(self, data_paths):

        self.df = self.preprocessFiles(data_paths)
        print("Data Preprocessing Done")



    def get_average_word_embedding(self, category, word2vec_model):
        # Splitting by both spaces and underscores
        words = category.replace('_', ' ').split()
        words = [x.lower() for x in words]
        embeddings = []
        for word in words:
            if word in word2vec_model:
                embeddings.append(word2vec_model[word])
            else:
                print(f"Word not found in the model's vocabulary: {word}")
        if embeddings:
            return np.mean(embeddings, axis=0)
        else:
            # Return a zero vector if none of the words were in the model's vocabulary
            return np.zeros(word2vec_model.vector_size)

    def clean_text(self, text):
        text = re.sub(self.regex_pattern, " ", text)  # Remove content within < and >
        text = html.unescape(text)  # Replace HTML character entities
        return text
    
    def preprocessFiles(self, data_paths):
        df = pd.concat([pd.read_csv(data_path) for data_path in data_paths], ignore_index=True)

        # Create dummy variables from the 'marketplace_id' column
        dummy_df = pd.get_dummies(df['marketplace_id'], prefix='marketplace')

        # Concatenate the dummy variables with the original DataFrame
        df = pd.concat([df, dummy_df], axis=1)

        # Remove the 'marketplace_id' column from the DataFrame
        df.drop('marketplace_id', axis=1, inplace=True)

        with open('data/category.json', 'r') as file:
            category_data = json.load(file)

        # Create a dictionary to map category IDs to category names
        category_mapping = {category['id']: category['name'] for category in category_data}

        # Map the integers in 'product_category_id' to category names
        df['product_category_name'] = df['product_category_id'].map(category_mapping)
        
        model_glove_twitter = api.load("glove-twitter-25")

        df['product_category_embed'] = df['product_category_name'].apply(lambda x: self.get_average_word_embedding(x, model_glove_twitter))
    
        self.regex_pattern = r"<.*?>"

        df['review_body'] = df['review_body'].apply(self.clean_text)
        df['review_body_decoded'] = df['review_body'].apply(unidecode)

        self.nReviews = df.shape[0]
        return df

        
    def textSplitter(self, string, max_length=500):
        if len(string) <= max_length:
            return [string]

        substrings = []
        while len(string) >= 5000:
            
            # find last '.' before 5000
            split_pos = string[:5000].rfind('.')
            # if no period found, split at last space
            if split_pos== -1:
                split_pos= string[:5000].rfind(' ')
                # if no space found, split at 5000
                if split_pos== -1:
                    split_pos = 5000

            substrings.append(string[:split_pos])
            string = string[split_pos:]
        substrings.append(string)
        return substrings

    def translator(self, text):   
        
        split_text = self.textSplitter(text)
        translation = ''
        for text in split_text:
            new_text = GoogleTranslator(source='auto', target='en').translate(text)
            if type(new_text) == str:  # If the translation was successful
                translation += new_text


        self.translations.append(translation)
        self.counter += 1

        if self.counter % 100 == 0:
            speed = self.counter/(time()-self.startTime)
            remainingTime = (self.nReviews-self.counter)/speed
            
            print(self.counter, '/', self.nReviews, 'translations done |', round(speed,2) , 'Rows/s | Est. time remaining:', int(remainingTime), 's | Saving to disk.')
            df1 = pd.DataFrame(self.translations, columns=['translation'])
            df1.to_csv('data/translations.csv', index=False)
        
        return translation

    def translateText(self):
        self.startTime = time()
        self.counter = 0  # Initialize a counter
        self.translations = []  # Initialize a list to store translations

        self.df['review_body_translated'] = self.df['review_body_decoded'].apply(self.translator)
        
        print(self.counter, 'of', self.nReviews, 'translations done. Saving to disk.')


if __name__ == "__main__":
    data_path = ['data/train-1.csv', 'data/train-2.csv', 'data/train-3.csv', 'data/train-4.csv', 'data/train-5.csv', 'data/train-6.csv', 'data/train-7.csv', 'data/train-8.csv']
    data_translator = DataTranslator(data_path)
    data_translator.translateText()
    
    # Save the translated data to a CSV file
    # data_translator.df.to_csv('data/translated_data.csv', index=False)