<h3 align="center"> Tweet Sentiment Classification on Imbalanced Datasets using Ensemble & Stacking Methods </h3>

<h4 align="center">Assignment 2 - Machine Learning - COMP30027 </h4>
<h4 align="center"> COMP30027 - Machine Learning </h4>

### Excerpt
> ...Twitter is a popular social media service in
which users post to their respective audiences
(followers) with what are called “tweets”.(N´adia
F.F. da Silva, 2014) These tweets can be anything the user deems ‘post-worthy’, with the
only significant constraint being their limitation to 140 characters (N´adia F.F. da Silva,
2014). Within these tweets, users can express sentiment respective to their content. It is this
sentiment and its prediction that will be the focus point of this report.
> 
> The aim of this report is to critically assess the effectiveness of various Machine
Learning classification algorithms and present empirical evidence and discussion regarding
the effectiveness of ensemble methods in this domain and how they were applied in this setting...

---

### Project Files

The project consists of labelled training data, and unlabelled test data. 
The test data is used to assess predictions and model performance via the Kaggle competition.

#### Modelling
`classificaion.ipynb` - The working notebook used for training and assessing performance of classifiers.  
`functions.py` - Helper functions including tweet preprocessing, cleaning and label operations.  

#### Data
`train.csv` - Raw tweets with labelled sentiments provided for model training purposes.  
`test.csv` - Unlabelled raw tweets used for Kaggle competition prediction submissions.  

#### Output Files
`Predictions.csv` - Classifier predictions for the `test.csv` tweets to be submitted to Kaggle.  


