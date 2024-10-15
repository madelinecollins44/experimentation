WITH experiments AS (
  SELECT experiment_id, MAX(_date) AS _date
  FROM `etsy-data-warehouse-prod.catapult.catapult_metrics_results` 
  WHERE _date >= "2024-01-01" 
    AND platform in ('iOS','Android') -- only BOE experiments 
  GROUP BY 1
),

tablet_metrics as (
  select
    _date, 
    experiment_id,
    lower(name) as metric_name,
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
    experiments USING (_date, experiment_id)
  inner join 
    etsy-data-warehouse-prod.etsy_atlas.catapult_metrics using (metric_id)
  where 
    lower(name) in ('conversion rate','winsorized ac*v','orders per unit','winsorized aov','mean engaged_visit','mean visits')  -- CR, ACBV, Orders, AOV and mean visits
    and _date > '2024-01-01'
    AND segmentation = "is_tablet"
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
  case when is_tablet = 'tablet' and metric_name in ('conversion rate') then p_value end as tablet_cr_pval,
  case when is_tablet = 'tablet' and metric_name in ('conversion rate') then percent_change end as tablet_cr_percent_change,
  case when is_tablet = 'tablet' and metric_name in ('conversion rate') then _power end as tablet_cr_power,
  
  case when is_tablet = 'tablet' and metric_name in ('winsorized ac*v') then p_value end as tablet_acxv_pval,
  case when is_tablet = 'tablet' and metric_name in ('winsorized ac*v') then percent_change end as tablet_acxv_percent_change,
  case when is_tablet = 'tablet' and metric_name in ('winsorized ac*v') then _power end as tablet_acxv_power,

  case when is_tablet = 'tablet' and metric_name in ('orders per unit') then p_value end as tablet_opu_pval,
  case when is_tablet = 'tablet' and metric_name in ('orders per unit') then percent_change end as tablet_opu_percent_change,
  case when is_tablet = 'tablet' and metric_name in ('orders per unit') then _power end as tablet_opu_power,

  case when is_tablet = 'tablet' and metric_name in ('winsorized aov') then p_value end as tablet_aov_pval,
  case when is_tablet = 'tablet' and metric_name in ('winsorized aov') then percent_change end as tablet_aov_percent_change,
  case when is_tablet = 'tablet' and metric_name in ('winsorized aov') then _power end as tablet_aov_power,

  case when is_tablet = 'tablet' and metric_name in ('mean engaged_visit') then p_value end as tablet_engaged_visits_pval,
  case when is_tablet = 'tablet' and metric_name in ('mean engaged_visit') then percent_change end as tablet_engaged_visits_percent_change,
  case when is_tablet = 'tablet' and metric_name in ('mean engaged_visit') then _power end as tablet_engaged_visits_power,

  case when is_tablet = 'tablet' and metric_name in ('mean visits') then p_value end as tablet_visits_pval,
  case when is_tablet = 'tablet' and metric_name in ('mean visits') then percent_change end as tablet_visits_percent_change,
  case when is_tablet = 'tablet' and metric_name in ('mean visits') then _power end as tablet_visits_power,
 
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
left join 
  etsy-data-warehouse-prod.etsy_atlas.catapult_launches b
  on a.experiment_id=b.launch_id
