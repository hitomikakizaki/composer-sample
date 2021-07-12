SELECT
  event_date,
  event_name,
  (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key='page_location') AS page_location,
  (SELECT ep.value.string_value FROM UNNEST(event_params) ep WHERE ep.key='page_title') AS page_title,
  user_pseudo_id,
  device.category,
  geo.country,
  geo.region,
  traffic_source.medium,
  traffic_source.source
FROM
  `{{ params.gcppj }}.{{ params.gcpds }}.events_{{params.yesterday}}` # パラメータ設定