from serra.python.base import PythonTransformer

from fuzzywuzzy import fuzz
import pandas as pd

class MatchTransformer(PythonTransformer):
    """
    Drop rows that have nulls in specified column(s).

    :param columns: The columns to look for
    """

    def __init__(self, columns_to_match, partition, include_fuzzy = True, threshold = 90):
        self.columns_to_match = columns_to_match # Dict: key = left column, value = right
        self.partition = partition
        self.include_fuzzy = include_fuzzy
        self.threshold = threshold
        
    def transform(self, left_df, right_df):

        matched_data = pd.DataFrame()

        for partition in left_df[self.partition].unique():

            left_df_subset = left_df[left_df[self.partition] == partition].copy()
            right_df_subset = right_df[right_df[self.partition] == partition].copy()

            #_______Exact Matches________
            for left_column, right_column in self.columns_to_match.items():
                exact_match_name = pd.merge(left_df_subset, right_df_subset,
                                            left_on=[left_column],
                                            right_on=[right_column],
                                            how='inner',
                                            suffixes=('_left', '_right'))

                exact_match_name[left_column + '_match'] = 1
                
                matched_data = pd.concat([matched_data, exact_match_name], ignore_index=True)
                matched_data['fuzzy_score'] = None
                print(f"\n\n\n\n————————Exact Matched Data for {left_column}——————————", matched_data['fuzzy_score'], "\n\n\n\n")
            
                # Remove matched rows
                left_df_subset = left_df_subset[~left_df_subset[left_column].isin(exact_match_name[left_column])]
                right_df_subset = right_df_subset[~right_df_subset[right_column].isin(exact_match_name[right_column])]

            #______Fuzzy Matches_______
            if self.include_fuzzy:

                match_series_list = []

                    # Iterate through each left-right column match pair
                        #  Iterate through each row in left_df
                            # For each single row in left_df, iterate through each right_df row
                                # Calculate fuzzy score for each row in the right_df to the single row in left_df
                for left_column, right_column in self.columns_to_match.items():

                        
                    for index, row in left_df_subset.iterrows():

                        fuzzy_scores = []

                            
                        for index_right, row_right in right_df_subset.iterrows():

                            # Compare the single left row value to all the right_df row values and get scores
                            # Store index of right_df value and score (all in comparison to single left row value)
                            score = fuzz.token_set_ratio(row[left_column], row_right[right_column])
                            fuzzy_scores.append((index_right, score))

                        # Proceed only if there are scores to evaluate
                        if fuzzy_scores:
                            # Find the max score and corresponding index
                            max_score_index, max_score = max(fuzzy_scores, key=lambda x: x[1])

                            # If the max score is above a threshold, consider it a match
                            
                            # Row is a single column where each index is the column name and column value is the value
                            if max_score > self.threshold:
                                match_dict = {
                                    **row.to_dict(), # Dict where key = column, pair = value
                                    **right_df_subset.loc[max_score_index].to_dict(),
                                    f"{left_column}_match": -1,  # Marking the match with a special flag
                                    'fuzzy_score': max_score
                                }

                                match_series = pd.Series(match_dict, name=index) # Single column series where values are just pair from dict, where series index labels are keys from dict(columns) 
                                print(f"\n\n\n\n————————Row for {left_column}——————————", row, "\n\n\n\n")
                                print(f"\n\n\n\n————————Matched Series Dict for {left_column}——————————", match_series, "\n\n\n\n")
                                match_series_list.append(match_series.to_frame().T)  # Convert Series to DataFrame where single column w/ indexes of column names, values of pairs, signle column becomes single row where headers are column names
                                print(f"\n\n\n\n————————Transpose Match for {left_column}——————————", match_series.to_frame().T, "\n\n\n\n")
                                print(f"\n\n\n\n————————Fuzzy Matched Series List for {left_column}——————————", match_series_list, "\n\n\n\n")

                                # Remove matched row from left_df_subset and right_df_subset
                                left_df_subset = left_df_subset.drop(index)
                                right_df_subset = right_df_subset.drop(max_score_index)

                # After iterating through all columns and rows, check if there are any matches to append
                if match_series_list:  # Check if the list is not empty
                    matched_data = pd.concat([matched_data] + match_series_list, ignore_index=True)
                
                print(f"\n\n\n\n————————Joined Matched Data for {left_column}——————————", matched_data['fuzzy_score'], "\n\n\n\n")

        return matched_data

