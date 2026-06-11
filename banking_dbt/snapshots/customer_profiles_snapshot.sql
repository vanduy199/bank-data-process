{% snapshot customer_profiles_snapshot %}
{{
    config(
      target_schema='ANALYTICS',
      unique_key='customer_id',
      strategy='timestamp',
      updated_at='updated_at'
    )
}}

select * from {{ ref('stg_customer_profiles') }}

{% endsnapshot %}
