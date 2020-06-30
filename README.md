# Normalise CSV file

Kaggle-like data set are usually a few hundred megabytes or more, available as CSV files.
Other example is the [Dataset of Travis CI and Google Testing Results](https://github.com/elbaum/CI-Datasets).

CSV is a plain human friendly storage format. It is however inefficient in terms of space and processing.
This repo contains a script to [normalize dataset](https://en.wikipedia.org/wiki/Database_normalization#:~:text=Database%20normalization%20is%20the%20process,part%20of%20his%20relational%20model).

Concretely:

- GooglePresCleanData.out.zip  	66MB	small, unusable
- GooglePresCleanData.out.txt  	481MB	big usable
- GooglePresCleanData.out.sqlite  	50MB	smallest, most usable

- RailsCleanData.out.zip  	150MB	small, unusable
- RailsCleanData.out.txt  	2.3GB	big, usable
- RailsCleanData.out.sqlite  	319MB	small, most usable (& can be manually normalised further)
