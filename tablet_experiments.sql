WITH experiments AS (
  SELECT experiment_id, MAX(_date) AS _date
  FROM `etsy-data-warehouse-prod.catapult.catapult_metrics_results` 
  WHERE _date >= "2024-01-01" 
    AND platform in ('iOS','Android') -- only BOE experiments 
  GROUP BY all
)
, tablet_metrics as (
  select
    _date, 
    experiment_id,
    lower(name) as metric_name,
    metric_id,
    platform,
    IF(segment = "1", "tablet", "not_tablet") AS is_tablet,
    population_1_name as control,
    population_2_name as treatment,
    p_value, 
    power as _power, 
    percent_change,
  from 
    `etsy-data-warehouse-prod.catapult.catapult_metrics_results` 
  INNER JOIN 
    experiments USING (_date, experiment_id) -- get experiment metrics for max day of experiment 
  inner join 
    etsy-data-warehouse-prod.etsy_atlas.catapult_metrics using (metric_id)
  where 
    1=1
    and _date >= '2024-01-01'
    and segmentation = "is_tablet"
    -- and lower(name) in ('conversion rate','winsorized acvv','orders per unit','winsorized aov','mean engaged_visit','mean visits', 'mean total_winsorized_gms')  -- CR, ACBV, Orders, AOV and mean visits
    and metric_id in ( 
    1029227163677, 9542279976, -- cr
    1029227163605,653265516272, --acvv
    1303188452741, -- orders per unit
    1177416980549, 1303200836305,-- aov
    47362065341,1016362090850, -- mean visits 
    1288963657822,1304192866864-- engaged visits 
    )
  group by all 
)
select
  a._date,
  a.experiment_id,
  b.name as experiment_name,
  b.team as squad,
  b.initiative,
  control,
  treatment,
  -- tablet results
  case when is_tablet = 'tablet' and metric_id in (1029227163677, 9542279976) then p_value end as tablet_cr_pval,
  case when is_tablet = 'tablet' and metric_id in (1029227163677, 9542279976) then percent_change end as tablet_cr_percent_change,
  case when is_tablet = 'tablet' and metric_id in (1029227163677, 9542279976)  then _power end as tablet_cr_power,
  
  case when is_tablet = 'tablet' and metric_id in (1029227163605, 653265516272)  then p_value end as tablet_acxv_pval,
  case when is_tablet = 'tablet' and metric_id in (1029227163605, 653265516272) then percent_change end as tablet_acxv_percent_change,
  case when is_tablet = 'tablet' and metric_id in (1029227163605, 653265516272) then _power end as tablet_acxv_power,

  case when is_tablet = 'tablet' and metric_id in (1303188452741) then p_value end as tablet_opu_pval,
  case when is_tablet = 'tablet' and metric_id in (1303188452741) then percent_change end as tablet_opu_percent_change,
  case when is_tablet = 'tablet' and metric_id in (1303188452741) then _power end as tablet_opu_power,

  case when is_tablet = 'tablet' and metric_id in (1177416980549, 1303200836305) then p_value end as tablet_aov_pval,
  case when is_tablet = 'tablet' and metric_id in (1177416980549, 1303200836305) then percent_change end as tablet_aov_percent_change,
  case when is_tablet = 'tablet' and metric_id in (1177416980549, 1303200836305) then _power end as tablet_aov_power,

  case when is_tablet = 'tablet' and metric_id in (1288963657822,1304192866864) then p_value end as tablet_engaged_visits_pval,
  case when is_tablet = 'tablet' and metric_id in (1288963657822,1304192866864) then percent_change end as tablet_engaged_visits_percent_change,
  case when is_tablet = 'tablet' and metric_id in (1288963657822,1304192866864) then _power end as tablet_engaged_visits_power,

  case when is_tablet = 'tablet' and metric_id in (47362065341,1016362090850) then p_value end as tablet_visits_pval,
  case when is_tablet = 'tablet' and metric_id in (47362065341,1016362090850)  then percent_change end as tablet_visits_percent_change,
  case when is_tablet = 'tablet' and metric_id in (47362065341,1016362090850)  then _power end as tablet_visits_power,
 
  -- not tablet results 
  case when is_tablet = 'not tablet' and metric_name in ('conversion rate') then p_value end as not_tablet_cr_pval,
  case when is_tablet = 'not tablet' and metric_name in ('conversion rate') then percent_change end as not_tablet_cr_percent_change,
  case when is_tablet = 'not tablet' and metric_name in ('conversion rate') then _power end as not_tablet_cr_power,
  
  case when is_tablet = 'not tablet' and metric_name in ('winsorized ac*v') then p_value end as not_tablet_acxv_pval,
  case when is_tablet = 'not tablet' and metric_name in ('winsorized ac*v') then percent_change end as not_tablet_acxv_percent_change,
  case when is_tablet = 'not tablet' and metric_name in ('winsorized ac*v') then _power end as not_tablet_acxv_power,

  case when is_tablet = 'not_tablet' and metric_name in ('orders per unit') then p_value end as not_tablet_opu_pval,
  case when is_tablet = 'not_tablet' and metric_name in ('orders per unit') then percent_change end as not_tablet_opu_percent_change,
  case when is_tablet = 'not_tablet' and metric_name in ('orders per unit') then _power end as not_tablet_opu_power,

  case when is_tablet = 'not_tablet' and metric_name in ('winsorized aov') then p_value end as not_tablet_aov_pval,
  case when is_tablet = 'not_tablet' and metric_name in ('winsorized aov') then percent_change end as not_tablet_aov_percent_change,
  case when is_tablet = 'not_tablet' and metric_name in ('winsorized aov') then _power end as not_tablet_aov_power,

  case when is_tablet = 'not_tablet' and metric_name in ('mean engaged_visit') then p_value end as not_tablet_engaged_visits_pval,
  case when is_tablet = 'not_tablet' and metric_name in ('mean engaged_visit') then percent_change end as not_tablet_engaged_visits_percent_change,
  case when is_tablet = 'not_tablet' and metric_name in ('mean engaged_visit') then _power end as not_tablet_engaged_visits_power,

  case when is_tablet = 'not_tablet' and metric_name in ('mean visits') then p_value end as not_tablet_visits_pval,
  case when is_tablet = 'not_tablet' and metric_name in ('mean visits') then percent_change end as not_tablet_visits_percent_change,
  case when is_tablet = 'not_tablet' and metric_name in ('mean visits') then _power end as not_tablet_visits_power

from 
  tablet_metrics a
inner join 
  etsy-data-warehouse-prod.etsy_atlas.catapult_launches b
  on a.experiment_id=b.launch_id
where launch_id = 1305470782469


------------------------------------------------
  --KELLYS VERSION
------------------------------------------------
WITH experiments AS (
  SELECT experiment_id, MAX(_date) AS _date
  FROM `etsy-data-warehouse-prod.catapult.catapult_metrics_results` 
  WHERE _date >= "2024-01-01" 
    AND platform in ('iOS','Android') -- only BOE experiments 
  GROUP BY all
)
, tablet_metrics as (
WITH experiments AS (
  SELECT experiment_id, MAX(_date) AS _date
  FROM `etsy-data-warehouse-prod.catapult.catapult_metrics_results` 
  WHERE _date >= "2024-01-01" 
    AND platform in ('iOS','Android') -- only BOE experiments 
  GROUP BY 1
)
  
select
  _date, 
  experiment_id,
  lower(name) as metric_name,
  metric_id,
  platform,
  IF(segment = "1", "tablet", "not_tablet") AS is_tablet,
  population_1_name as control,
  population_2_name as treatment,
  p_value, 
  power as _power, 
  percent_change,
from 
  `etsy-data-warehouse-prod.catapult.catapult_metrics_results` 
INNER JOIN experiments USING (_date, experiment_id)
inner join etsy-data-warehouse-prod.etsy_atlas.catapult_metrics using (metric_id)
where 
  _date > '2024-01-01'
  AND segmentation = "is_tablet"
  and experiment_id = 1305470782469
  AND metric_id IN (
    9542279976, # Conversion rate (non-CUPED)
    653265516272, # Winsorized AC*V (non-CUPED)
    1304192866864, # Mean engaged visits
    47362065341, # Mean visits (non-CUPED)
    1303188452741, # Orders per unit
    1303200836305 # Winsorized AOV
  )
)
select 
  a._date,
  a.experiment_id,
  b.name as experiment_name,
  b.team as squad,
  b.initiative,
  control,
  treatment,
  is_tablet,
  -- tablet results
  max(case when metric_id in (9542279976) then p_value end) as cr_pval,
  max(case when metric_id in (9542279976) then percent_change end) as cr_percent_change,
  max(case when metric_id in (9542279976) then _power end) as cr_power,

  max(case when metric_id in (653265516272) then p_value end) as acvv_pval,
  max(case when metric_id in (653265516272) then percent_change end) as acvv_percent_change,
  max(case when metric_id in (653265516272) then _power end) as acvv_power,

  max(case when metric_id in (1304192866864) then p_value end) as mean_engaged_visits_pval,
  max(case when metric_id in (1304192866864) then percent_change end) as mean_engaged_visits_percent_change,
  max(case when metric_id in (1304192866864) then _power end) as mean_engaged_visits_power,

  max(case when metric_id in (47362065341) then p_value end) as mean_visits_pval,
  max(case when metric_id in (47362065341) then percent_change end) as mean_visits_percent_change,
  max(case when metric_id in (47362065341) then _power end) as mean_visits_power,

  max(case when metric_id in (1303188452741) then p_value end) as opu_pval,
  max(case when metric_id in (1303188452741) then percent_change end) as opu_percent_change,
  max(case when metric_id in (1303188452741) then _power end) as opu_power,

  max(case when metric_id in (1303200836305) then p_value end) as aov_pval,
  max(case when metric_id in (1303200836305) then percent_change end) as aov_percent_change,
  max(case when metric_id in (1303200836305) then _power end) as aov_power

from 
  tablet_metrics a
inner join 
  etsy-data-warehouse-prod.etsy_atlas.catapult_launches b
  on a.experiment_id=b.launch_id
where launch_id = 1305470782469
group by all
