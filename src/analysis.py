
import pandas as pd
import matplotlib.pyplot as plt

class ReviewAnalysis:
    def analyze_sentiment_by_class(self, df: pd.DataFrame,
                class_column: str = "Class Name",
                sentiment_column: str = "AI Sentiment"
            ):
            """
            This functions wows:-
            1. Percentage breakdown of sentiments per clothing class
            2. Clothing classes with highest positive, negative, and neutral sentiment
            """

            # checks it the class column exists
            if class_column not in df.columns or sentiment_column not in df.columns:
                raise ValueError("Column name not found in dataframe")

            # Group the dataframe by the Cloth class name and Sentiment
            sentiment_counts = df.groupby([class_column, sentiment_column]).size().unstack(fill_value=0)

            # Convert count to percentages
            sentiment_pct = sentiment_counts.div(sentiment_counts.sum(axis=1), axis=0) * 100

            print("\n=== Sentiment Percentage Breakdown per Clothing Class ===")
            print(sentiment_pct.round(2))

            # check the index with maximum value for each sentiment  types
            if "positive" in sentiment_pct.columns:
                highest_positive = sentiment_pct["positive"].idxmax()
            else:
                highest_neutral = None
            if "negative" in sentiment_pct.columns:
                highest_negative = sentiment_pct["negative"].idxmax()
            else:
                highest_neutral = None

            if "neutral" in sentiment_pct.columns:
                highest_neutral = sentiment_pct["neutral"].idxmax()
            else:
                highest_neutral = None

            # highest_positive = sentiment_pct["positive"].idxmax()
            # highest_negative = sentiment_pct["negative"].idxmax()
            # highest_neutral = sentiment_pct["neutral"].idxmax()

            print("\n=== Highest Sentiment Classes ===")
            print(f"Highest POSITIVE sentiment: {highest_positive} ({sentiment_pct.loc[highest_positive, 'positive']:.2f}%)")
            print(f"Highest NEGATIVE sentiment: {highest_negative} ({sentiment_pct.loc[highest_negative, 'negative']:.2f}%)")
            print(f"Highest NEUTRAL sentiment:  {highest_neutral} ({sentiment_pct.loc[highest_neutral, 'neutral']:.2f}%)")

            # plot a vertical barchart
            sentiment_pct.plot(kind="barh", figsize=(10, 6))
            plt.title("Sentiment % by Clothing Class")
            plt.xlabel("Percentage (%)")
            plt.ylabel("Clothing Class")
            plt.xticks(rotation=90, ha="right")
            plt.tight_layout()
            plt.show()

            #return a dictionary of results

            return {
                "sentiment_percentages": sentiment_pct,
                "highest_positive": highest_positive,
                "highest_negative": highest_negative,
                "highest_neutral": highest_neutral,
            }
