# MAST30034 Project 1 README.md
- Name: Noah Sebastian
- Student ID: 911150

**Research Goal:** Forecast fare amounts using the subset of features that are present before a journey is undertaken.

**Timeline:** The timeline for the research area is 2019 (training) and 2021 (testing).

To run the pipeline, please visit the `notebooks` directory and run the files in order:
1. `download.ipynb`: This downloads the raw data into the `data/raw` directory.
3. `collate.ipynb`: This notebook combines the data sets using `collate.py`.
4. `clean.ipynb`: This notebook cleans the data sets using `clean.py`.
5. `analysis_preliminary.ipynb`: This notebook performs preliminary analysis on the uncleaned data set.
6. `analysis_time_series.ipynb`: This notebook performs some time series analysis.
7. `analysis_visualisation.ipynb`: This notebook performs and produces visual analysis for the report.
8. `build_evaluate_model.ipynb`: This notebook trains/reads/writes the model using `model.py` and performs evaluation.


**Requirements:** can be found in the `requirements.txt`.

METAR data can be downloaded from:
https://mesonet.agron.iastate.edu/request/download.phtml?network=NY_ASOS