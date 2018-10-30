-- This generates all the contracts old enough to be informative for the rescoop report
-- Usage: sql2csv.py -C dbconfig.py informative_contracts.sql --minyears 2
select
    contract.id
from giscedata_polissa as contract
where
    extract( year from now()) - extract(year from contract.data_alta) > %(minyears)s and
    contract.active and
    true
