{% snapshot scd_raw_listings %}
{{
    config(
        target_schema='DEV_MICHAEL',
        unique_key='id',
        strategy='check',
        check_cols=['name', 'minimum_nights'],
        invalidate_hard_deletes=True
    )
}}
SELECT *
FROM {{ source('airbnb_michael', 'listings') }}
{% endsnapshot %}