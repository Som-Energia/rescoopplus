#!/bin/bash

# This script generates a list of contracts ids, suitable to be included
# in the rescoop report.
#
# "excluded_contracts.csv" should be obtained from the "num de contracte"
# column of the google form to opt-out "Negativa a la comunicació
# de dades de Som Energia a BeeData (respuestas)"


sql2csv.py contract_clusters.sql -C dbconfig.py --minyears 2 | sort -g >  contract_clusters.csv

(
    sql2csv.py informative_contracts.sql -C dbconfig.py --minyears 2 |
    ./filterids.py excluded_contracts.csv |
    sort -g
)> rescoop_contracts.csv

