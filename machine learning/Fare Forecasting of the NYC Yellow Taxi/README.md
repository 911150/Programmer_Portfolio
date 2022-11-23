<h3 align="center">An application and analysis of fare forecasting using Gradient Boosted Trees against Random Forests applied to the New York City Yellow Cab</h3>
<h4 align="center"> Project 1 - Applied Data Science - MAST30034</h4>
<h4 align="center">Noah Sebastian | 911150</h4>

**Research Goal:** Forecast fare amounts of the New York City Yellow Taxi Cab using the subset of features that are present before a journey is undertaken.

**Timeline:** The timeline for the research from 2019-2022. The training data will consist of trips undertaken from 
Jan-Dec of 2019 and will be validated against trips from 2021-22 

To run the pipeline, please visit the `notebooks` directory and run the files in order:
1. `download.ipynb`: This downloads the raw data into the `data/raw` directory.
2. `collate_data.ipynb`: This notebook combines the data sets using `collate.py`.
3. `analysis_preliminary.ipynb`: This notebook performs preliminary analysis on the uncleaned data set.
4. `outlier_removal.ipynb`: This notebook cleans the data sets using `clean.py` based on findings of the preliminary analysis.
5. `analysis_time_series.ipynb`: This notebook performs time series analysis of the features.
6. `analysis_visualisation.ipynb`: This notebook performs and produces visual analysis for the report.
7. `build_evaluate_model.ipynb`: This notebook trains/reads/writes the model using `model.py` and performs evaluation.

**Excerpt from report:**
> ...An aspect of the New York City (NYC) taxi service, and one that differs from the more modern
on-demand-vehicles, is their inability to present end-to-end fare prices and inform individuals pre-travel
about their future incurred costs. This is an area where for-hire-vehicles (FHV’s) have a significant
competitive advantage over the taxi industry.
> 
> As such, this report will assume the perspective of a research and development company working
on behalf of the New York City Taxi & Limousine Commission (NYCTLC) to provide insights and
prospectus integration’s for the current NYCTLC system and, more specifically, will attempt to offer
a method for forecasting trip fare amounts for varying circumstances around New York City.
> 
> For this task, a Gradient-Boosted-Tree Regressor is suggested as such a model since it is capable
of handling imperfect data sets, can generalize well and offers high flexibility. It is also currently
one of the more mainstream algorithms adopted by the data science community. It will be contrast
against a Random Forest Decision Tree Regressor, with the intention of exploring their differences,
and effectiveness’...


**Requirements:** can be found in the `requirements.txt`.

METAR data can be downloaded from:
https://mesonet.agron.iastate.edu/request/download.phtml?network=NY_ASOS