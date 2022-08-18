{{ config(
    materialized = 'incremental',
    unique_key = 'event_bundle_sequence_id'
) }}

with raw_time_calculations as (
    select 
        user_pseudo_id,
        min(event_timestamp) as page_view_start,
        max(event_timestamp) as max_collector_tstamp,
    from `bigquery-public-data`.`ga4_obfuscated_sample_ecommerce`.`events_20210104`
    where event_name = 'page_view'
    {% if is_incremental() %}
    and event_timestamp >= (select max(event_timestamp) from {{ this }})
    {% endif %}
    group by user_pseudo_id
),
time_calculations as (
    select
        timestamp_micros(page_view_start) as page_view_start,
        timestamp_micros(max_collector_tstamp) as max_collector_tstamp,
        (max_collector_tstamp - page_view_start) / 1e6 as approx_time_on_page
    from raw_time_calculations
)
select * from time_calculations
