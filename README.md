### Reader Survey Analysis

See our <a href="https://meta.wikimedia.org/wiki/Research:Characterizing_Wikipedia_Reader_Behaviour/Code">meta page</a> for a more complete description of the code contained within this repository.
The main goal of this code is to provide a means for debiasing survey results (to account for sample and non-response bias) based upon associated log data.

#### Overview
* src/preprocessing: code for cleaning survey responses and preparing them for de-biasing
    * 01_collectsurveydata: clean survey responses 
    * 02_extractlogtraces: extract and preprocess log data for debiasing
    * 03_debiassurveys: build model to compute new weights for each survey response based upon the estimated likelihood that they would respond to the survey
* src/utils: various utility scripts and configuration for the code
