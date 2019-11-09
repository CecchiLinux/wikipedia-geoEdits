# Wikipedia-geoEdits

This is tool designed for the study of the distribution of anonymous changes made to Wikipedia pages.

The software combines the [Wikipedia edit history](https://snap.stanford.edu/data/wiki-meta.html) (Wikipedia ENG: TODO number of contributions) with the [ip2Location LITE database](https://lite.ip2location.com/database/ip-country-region-city-latitude-longitude-zipcode) associating each anonymous contribution with the corresponding geographic coordinates. This is possible because anonymous editors are listed by their ip address, e.g. ip:69.17.21.242.
The IP2Location LITE database provide 2.920.499 distinct location all over the world for the IP resolution.

In this study I decided to consider only the geographic coordinates and then use a clustering algorithm to perform the grouping of areas based on the concentration of the points. The distribution of the categories of interest in the different areas of the world is thus studied.

It is possible to filter categories and sub-categories and than visualize the results on a map.



![map](/home/enrico/datasets/filters-software-game/map.png)



## Getting Started

- download the [Wikipedia edit history](https://snap.stanford.edu/data/wiki-meta.html) 
- download the [ip2Location LITE database](https://lite.ip2location.com/database/ip-country-region-city-latitude-longitude-zipcode)
- convert the Wikipedia edit history file format to make it friendly to Spark

```bash
bzcat /path_to_dataset/enwiki-20080103.main.bz2 | python3 ./python/enwiki2csv.py | python3 ./python/ip2integer.py | bzip2 > /path_to_dataset/enwiki-longIpOnly.bz2
```



### Usage

On the first run you need to generate the category to coordinates file using the option [-a], than you can exclude that phase.

```bash
usage: ./run.sh [-a] [--dataset_folder <folder path>] [--words <required words>...]
	 	 [-k --k  <clusters>] [-i, --iterations  <arg>]

options:
  -a, --associate_location              Perform the association phase
  -d, --dataset_folder  <folder path>  		Resources files folder
  -e, --epsilon  <arg>                  Variance improvement threshold
  -i, --iterations  <arg>               Number of iterations that the clustering algorithm will be run for.
  -k, --k  <clusters>                   Number of clusters to create.
  -m, --master_url  <Master URL>      Master URL
  -n, --no_words  <excluded words>...   Comma separated excluded words.
  -p, --print_numbers                   Print the numbers on the map
  -w, --words  <required words>...      Comma separated required words.
  -h, --help                            Show help message
```

