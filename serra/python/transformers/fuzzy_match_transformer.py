from serra.python.base import PythonTransformer

from fuzzywuzzy import fuzz
import nltk
from nltk.util import ngrams
import pandas as pd

def remove_phrases(text, phrases_to_remove):
    for phrase in phrases_to_remove:
        text = text.replace(phrase, '')
    return text

class FuzzyMatchTransformer(PythonTransformer):
    """
    Drop rows that have nulls in specified column(s).

    :param columns: The columns to look for
    """

    def __init__(self, left_df, columns_to_match, partition):
        self.left_df = left_df # facilities
        self.columns_to_match = columns_to_match
        self.partition = partition
        
    def transform(self, right_df):

        if 'name' in self.columns_to_match:
            all_tokens = [token for sublist in self.left_df['tokenized_name'].tolist() + right_df['tokenized_name'].tolist() for token in sublist]

            trigrams = list(ngrams(all_tokens, 3))
            bigrams = list(ngrams(all_tokens, 2))

            trigram_freq = nltk.FreqDist(trigrams)
            bigram_freq = nltk.FreqDist(bigrams)

            phrases_to_remove = [' '.join(trigram) for trigram, freq in trigram_freq.most_common(3)]
            phrases_to_remove += [' '.join(bigram) for bigram, freq in bigram_freq.most_common(2)]

        matched_data = pd.DataFrame()

        # Iterate over each unique state
        for state in self.left_df[self.partition].unique():
            print(state)
            # Subset the data for the current state
            df_state_facilities = self.left_df[self.left_df[self.partition] == state].copy()
            df_state_provider_info = right_df[right_df[self.partition] == state].copy()

            # Step 1: Exact match on state and name
            for left_column, right_column in self.columns_to_match.items():
                exact_match_name = pd.merge(df_state_facilities, df_state_provider_info,
                                            left_on=[left_column],
                                            right_on=[right_column],
                                            how='inner',
                                            suffixes=('_fac', '_prov'))

                exact_match_name[left_column + '_match'] = 1

                matched_data = pd.concat([matched_data, exact_match_name], ignore_index=True)
                print(len(matched_data))

                # Remove matched rows
                df_state_facilities = df_state_facilities[~df_state_facilities[left_column].isin(exact_match_name[left_column])]
                df_state_provider_info = df_state_provider_info[~df_state_provider_info[right_column].isin(exact_match_name[right_column])]

            # Step 3: Fuzzy matching on address for remaining rows

            match_series_list = []

            for left_column, right_column in self.columns_to_match.items():
                for index, row in df_state_facilities.iterrows():
                    # Initialize a list to store scores for each provider row against the current facility row
                    address_scores = []

                    for index_prov, row_prov in df_state_provider_info.iterrows():
                        # Calculate fuzzy match score for the current column
                        score = fuzz.token_set_ratio(row[left_column], row_prov[right_column])
                        address_scores.append((index_prov, score))

                    # Proceed only if there are scores to evaluate
                    if address_scores:
                        # Find the max score and corresponding index
                        max_score_index, max_score = max(address_scores, key=lambda x: x[1])

                        # If the max score is above a threshold, consider it a match
                        
                        if max_score > 93:
                            match_dict = {
                                **row.to_dict(),
                                **df_state_provider_info.loc[max_score_index].to_dict(),
                                f"{left_column}_match": -1,  # Marking the match with a special flag
                                'fuzzy_score': max_score
                            }

                            match_series = pd.Series(match_dict, name=index)
                            match_series_list.append(match_series.to_frame().T)  # Convert Series to DataFrame and append

                            # Remove matched row from df_state_facilities and df_state_provider_info
                            df_state_facilities = df_state_facilities.drop(index)
                            df_state_provider_info = df_state_provider_info.drop(max_score_index)

            # After iterating through all columns and rows, check if there are any matches to append
            if match_series_list:  # Check if the list is not empty
                matched_data = pd.concat([matched_data] + match_series_list, ignore_index=True)
        return matched_data

