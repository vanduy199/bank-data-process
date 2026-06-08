{% snapshot services_snapshot %}
{{
    config(
      target_schema='ANALYTICS',
      unique_key='service_id',
      strategy='check',
      check_cols=['service_name', 'category', 'is_active']
    )
}}

SELECT * FROM {{ ref('stg_services') }}

{% endsnapshot %}
