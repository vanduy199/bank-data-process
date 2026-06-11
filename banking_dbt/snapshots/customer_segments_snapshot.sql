{% snapshot customer_segments_snapshot %}
{{
    config(
      target_schema='ANALYTICS',
      unique_key='customer_id',
      strategy='timestamp',
      updated_at='assigned_at'
    )
}}

select * from {{ ref('stg_customer_segments') }}

{% endsnapshot %}
