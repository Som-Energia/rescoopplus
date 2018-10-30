-- Clusterizes the contracts depending on several aspects considered in the rescoop report
-- like: domestic/comercial/industrial, autoproducer, energy efficiency actions and generation actions
-- Usage: sql2csv.py -C dbconfig.py contract_clusters.sql --minyears 2
select
    tarifa,
    is_home,
    isprosumer,
    has_infoenergia,
    has_generation,
    count(contract_id),
    max(contract_id),
    string_agg(contract_id::text,',') as ids,
    true
from (
    select
        max(tarifa.name) as tarifa,
        contract.id as contract_id,
        contract.autoconsumo='01' as isprosumer,
        count(gen_assignment.id)>0 as has_generation,
        count(infoenergia.id)>0 as has_infoenergia,
        contract.cnae=986 as is_home, -- id for 9820
        true
    from giscedata_polissa as contract
    left join giscedata_polissa_tarifa as tarifa
       on tarifa.id = contract.tarifa
    left join generationkwh_assignment as gen_assignment
       on contract.id = gen_assignment.contract_id
    left join empowering_customize_profile_channel_log as infoenergia
       on contract.id = infoenergia.contract_id
    where
        extract( year from now()) - extract(year from data_alta) > %(minyears)s and
        contract.active = true and
        true
    group by
        contract.id,
        cnae,
        tarifa.id,
        contract.autoconsumo,
        isprosumer,
        true
) as clusters
group by
    isprosumer,
    tarifa,
    is_home,
    has_infoenergia,
    has_generation
order by
    tarifa,
    is_home,
    isprosumer,
    has_infoenergia,
    has_generation
