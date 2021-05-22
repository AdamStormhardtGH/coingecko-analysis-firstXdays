select day_number, coin_id, coin_price from coin_day_launch_report365_normalised_csv cdlrnc 
where day_number = 364
group by day_number, coin_id, coin_price
limit 60;


