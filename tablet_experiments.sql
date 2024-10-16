------------------------------------------------------------------------------------------------
overall results
  --this is browser bucketed only. right now segmentations dont exist for user bucketed experiments
------------------------------------------------------------------------------------------------
WITH experiments AS (
  SELECT experiment_id, MAX(_date) AS _date
  FROM `etsy-data-warehouse-prod.catapult.catapult_metrics_results` a
  inner join `etsy-data-warehouse-prod.etsy_atlas.catapult_launches_expected_platforms` b
    on a.experiment_id=b.launch_id
  WHERE _date >= "2024-01-01" 
  AND lower(name) like ('%boe%') -- only BOE experiments 
  GROUP BY all
), tablet_metrics as (
select
  _date, 
  experiment_id,
  lower(name) as metric_name,
  metric_id,
  platform,
  -- IF(segment = "1", "tablet", "not_tablet") AS is_tablet,
  population_1_name as control,
  population_2_name as treatment,
  population_1_browser_count as control_count,
  population_2_browser_count as treatment_count,
  p_value, 
  power as _power, 
  percent_change,
  coalesce(significance,false) as significance
from 
  `etsy-data-warehouse-prod.catapult.catapult_metrics_results` 
INNER JOIN experiments USING (_date, experiment_id)
inner join etsy-data-warehouse-prod.etsy_atlas.catapult_metrics using (metric_id)
where 
  _date > '2024-01-01'
  AND segmentation = 'any'
  AND metric_id IN (
    1029227163677, # Conversion rate 
    1029227163605, # Winsorized AC*V 
    1288963657822, # Mean engaged visits
    1016362090850, # Mean visits
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
  -- is_tablet,
  max(control_count) as control_count,
  max(treatment_count) as treatment_count,
  max(case when metric_id in (1029227163677) then significance end) as cr_signficance,
  max(case when metric_id in (1029227163677) then p_value end) as cr_pval,
  max(case when metric_id in (1029227163677) then percent_change end) as cr_percent_change,
  max(case when metric_id in (1029227163677) then _power end) as cr_power,

  max(case when metric_id in (1029227163605) then significance end) as acvv_signficance,
  max(case when metric_id in (1029227163605) then p_value end) as acvv_pval,
  max(case when metric_id in (1029227163605) then percent_change end) as acvv_percent_change,
  max(case when metric_id in (1029227163605) then _power end) as acvv_power,

  max(case when metric_id in (1288963657822) then significance end) as mean_engaged_visits_signficance,
  max(case when metric_id in (1288963657822) then p_value end) as mean_engaged_visits_pval,
  max(case when metric_id in (1288963657822) then percent_change end) as mean_engaged_visits_percent_change,
  max(case when metric_id in (1288963657822) then _power end) as mean_engaged_visits_power,

  max(case when metric_id in (1016362090850) then significance end) as mean_visits_signficance,
  max(case when metric_id in (1016362090850) then p_value end) as mean_visits_pval,
  max(case when metric_id in (1016362090850) then percent_change end) as mean_visits_percent_change,
  max(case when metric_id in (1016362090850) then _power end) as mean_visits_power,

  max(case when metric_id in (1303188452741) then significance end) as opu_signficance,
  max(case when metric_id in (1303188452741) then p_value end) as opu_pval,
  max(case when metric_id in (1303188452741) then percent_change end) as opu_percent_change,
  max(case when metric_id in (1303188452741) then _power end) as opu_power,

  max(case when metric_id in (1303200836305) then significance end) as aov_signficance,
  max(case when metric_id in (1303200836305) then p_value end) as aov_pval,
  max(case when metric_id in (1303200836305) then percent_change end) as aov_percent_change,
  max(case when metric_id in (1303200836305) then _power end) as aov_power

from 
  tablet_metrics a
inner join 
  etsy-data-warehouse-prod.etsy_atlas.catapult_launches b
  on a.experiment_id=b.launch_id
group by all


------------------------------------------------------------------------------------------------
TABLET VS NON TABLET
  --this is browser bucketed only. right now segmentations dont exist for user bucketed experiments
------------------------------------------------------------------------------------------------
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
  population_1_browser_count as control_count,
  population_2_browser_count as treatment_count,
  p_value, 
  power as _power, 
  percent_change,
  coalesce(significance,false) as significance
from 
  `etsy-data-warehouse-prod.catapult.catapult_metrics_results` 
INNER JOIN experiments USING (_date, experiment_id)
inner join etsy-data-warehouse-prod.etsy_atlas.catapult_metrics using (metric_id)
where 
  _date > '2024-01-01'
  AND segmentation = "is_tablet"
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
  control_count,
  treatment_count,
  max(case when metric_id in (9542279976) then significance end) as cr_signficance,
  max(case when metric_id in (9542279976) then p_value end) as cr_pval,
  max(case when metric_id in (9542279976) then percent_change end) as cr_percent_change,
  max(case when metric_id in (9542279976) then _power end) as cr_power,

  max(case when metric_id in (653265516272) then significance end) as acvv_signficance,
  max(case when metric_id in (653265516272) then p_value end) as acvv_pval,
  max(case when metric_id in (653265516272) then percent_change end) as acvv_percent_change,
  max(case when metric_id in (653265516272) then _power end) as acvv_power,

  max(case when metric_id in (1304192866864) then significance end) as mean_engaged_visits_signficance,
  max(case when metric_id in (1304192866864) then p_value end) as mean_engaged_visits_pval,
  max(case when metric_id in (1304192866864) then percent_change end) as mean_engaged_visits_percent_change,
  max(case when metric_id in (1304192866864) then _power end) as mean_engaged_visits_power,

  max(case when metric_id in (47362065341) then significance end) as mean_visits_signficance,
  max(case when metric_id in (47362065341) then p_value end) as mean_visits_pval,
  max(case when metric_id in (47362065341) then percent_change end) as mean_visits_percent_change,
  max(case when metric_id in (47362065341) then _power end) as mean_visits_power,

  max(case when metric_id in (1303188452741) then significance end) as opu_signficance,
  max(case when metric_id in (1303188452741) then p_value end) as opu_pval,
  max(case when metric_id in (1303188452741) then percent_change end) as opu_percent_change,
  max(case when metric_id in (1303188452741) then _power end) as opu_power,

  max(case when metric_id in (1303200836305) then significance end) as aov_signficance,
  max(case when metric_id in (1303200836305) then p_value end) as aov_pval,
  max(case when metric_id in (1303200836305) then percent_change end) as aov_percent_change,
  max(case when metric_id in (1303200836305) then _power end) as aov_power

from 
  tablet_metrics a
inner join 
  etsy-data-warehouse-prod.etsy_atlas.catapult_launches b
  on a.experiment_id=b.launch_id
where experiment_id = 1258136522870
group by all



------------------------------------------------
--testing
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
  _date, 
  experiment_id,
  metric_id,
 is_tablet,
 count(*)
 from tablet_metrics
 group by all order by 5 desc
---this is all unique 
