import pytest
import pandas as pd
from src.analysis import ReviewAnalysis



def test_analyze_sentiment_by_class():

  test_df = pd.DataFrame({
        "Class Name": ["blouses","intimates","dresses","dresses","pants","blouses","blouses",'intimates',"pants","pants","blouses","blouses"],
        "AI Sentiment": ["positive","negative","negative","neutral",'positive',"positive",'positive',"positive","negative","negative","negative","negative"]
    })
  
  test_rev_obj  = ReviewAnalysis()

  test_result = test_rev_obj.analyze_sentiment_by_class(test_df)
  assert test_result.get("highest_positive") == "blouses"
  assert test_result.get("highest_negative") == "pants"


    
 