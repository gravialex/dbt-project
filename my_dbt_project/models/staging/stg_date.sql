{{ config(materialized='table') }}

with date_spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2000-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}

),

holidays as (
    select
        holiday_date::date as holiday_date
    from (
        values
            -- New Year's Day (Jan 1)
            ('2000-01-01'), ('2001-01-01'), ('2002-01-01'), ('2003-01-01'), ('2004-01-01'),
            ('2005-01-01'), ('2006-01-01'), ('2007-01-01'), ('2008-01-01'), ('2009-01-01'),
            ('2010-01-01'), ('2011-01-01'), ('2012-01-01'), ('2013-01-01'), ('2014-01-01'),
            ('2015-01-01'), ('2016-01-01'), ('2017-01-01'), ('2018-01-01'), ('2019-01-01'),
            ('2020-01-01'), ('2021-01-01'), ('2022-01-01'), ('2023-01-01'), ('2024-01-01'),
            ('2025-01-01'), ('2026-01-01'), ('2027-01-01'), ('2028-01-01'), ('2029-01-01'),
            ('2030-01-01'),

            -- Christmas Day (Dec 25)
            ('2000-12-25'), ('2001-12-25'), ('2002-12-25'), ('2003-12-25'), ('2004-12-25'),
            ('2005-12-25'), ('2006-12-25'), ('2007-12-25'), ('2008-12-25'), ('2009-12-25'),
            ('2010-12-25'), ('2011-12-25'), ('2012-12-25'), ('2013-12-25'), ('2014-12-25'),
            ('2015-12-25'), ('2016-12-25'), ('2017-12-25'), ('2018-12-25'), ('2019-12-25'),
            ('2020-12-25'), ('2021-12-25'), ('2022-12-25'), ('2023-12-25'), ('2024-12-25'),
            ('2025-12-25'), ('2026-12-25'), ('2027-12-25'), ('2028-12-25'), ('2029-12-25'),
            ('2030-12-25')
    ) as t (holiday_date)
),

final as (
    select
        date_day as "date",
        year(date_day) as year,
        quarter(date_day) as quarter,
        month(date_day) as month,
        day(date_day) as day,
        weekofyear(date_day) as week,
        dayofweek(date_day) as day_of_week, -- Note: Sunday is 0, Saturday is 6
        dayname(date_day) as day_name,
        monthname(date_day) as month_name,
        case when day_name in ('Sat', 'Sun') then true else false end as is_weekend
    from date_spine
)

select
    f."date",
    f.year,
    f.quarter,
    f.month,
    f.day,
    f.week,
    f.day_of_week,
    f.day_name,
    f.month_name,
    f.is_weekend,
    case when h.holiday_date is not null then true else false end as is_holiday
from final f
left join holidays h on f."date" = h.holiday_date
