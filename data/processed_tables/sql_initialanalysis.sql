select day_number, coin_id, coin_price 
from postgres.coin_analysis.coin_day_launch_report365_normalised_csv cdlrnc 
where day_number = 0
group by day_number, coin_id, coin_price
limit 60;



select 
day_number, 
coin_id,
case when coin_price != 0 then
	(
	coin_price - (FIRST_VALUE ( coin_price )  
	OVER ( 
	    partition by coin_id --[PARTITION BY partition_expression, ... ]
	    ORDER BY day_number asc
	) )
	) / (FIRST_VALUE ( coin_price )  
	OVER ( 
	    partition by coin_id --[PARTITION BY partition_expression, ... ]
	    ORDER BY day_number asc
	) ) *100
else null
end

as profit_percent
,coin_price as current_price


from coin_analysis.coin_day_launch_report365_normalised_csv cdlrnc 
--where coin_price is not null
where coin_id = 'goldblock'
--and coin_id not like '0-5x-%'

---where coin_id = 'shiba-inu'




---- get the median profit across all coins

with day5 as  ( 

select 
day_number, 
coin_id,
case when coin_price != 0 then
	(
	coin_price - (FIRST_VALUE ( coin_price )  
	OVER ( 
	    partition by coin_id --[PARTITION BY partition_expression, ... ]
	    ORDER BY day_number asc
	) )
	) / (FIRST_VALUE ( coin_price )  
	OVER ( 
	    partition by coin_id --[PARTITION BY partition_expression, ... ]
	    ORDER BY day_number asc
	) ) *100
else null
end

as profit_percent
,coin_price as current_price


from coin_analysis.coin_day_launch_report365_normalised_csv cdlrnc 

)
select sum(profit_percent)/726900 ,day_number
from day5
group by day_number
order by day_number asc
;

--limit 9

select count( distinct coin_id) from coin_analysis.coin_day_launch_report365_normalised_csv -- 7269 coins


--------
;

-- show me outrageous performers on day 5

with day5_calc as (
select 
day_number, 
coin_id,
case when coin_price != 0 then
	(
	coin_price - (FIRST_VALUE ( coin_price )  
	OVER ( 
	    partition by coin_id --[PARTITION BY partition_expression, ... ]
	    ORDER BY day_number asc
	) )
	) / (FIRST_VALUE ( coin_price )  
	OVER ( 
	    partition by coin_id --[PARTITION BY partition_expression, ... ]
	    ORDER BY day_number asc
	) ) *100
else null
end

as profit_percent
,coin_price as current_price


from coin_analysis.coin_day_launch_report365_normalised_csv cdlrnc 

)
select * from day5_calc 
where day_number = 5
and profit_percent >10000




----
-- show me coins which launched at $0
select * from coin_analysis.coin_day_launch_report365_normalised_csv
where coin_id = 'unagii-eth'



