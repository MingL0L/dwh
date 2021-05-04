CREATE TABLE IF NOT EXISTS public.stage_cur (
	date DATE NOT NULL,
	value FLOAT NOT NULL,
	serial_code VARCHAR NOT NULL,
	cur_code VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS public.dim_currency (
	cur_code VARCHAR NOT NULL,
	one_euro_value FLOAT NOT NULL,
	last_updated_date DATE NOT NULL,
	Serial_code VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS public.fact_exchange_rate_history (
	history_date DATE NOT NULL,
	from_cur_code VARCHAR NOT NULL,
	to_cur_code VARCHAR NOT NULL,
	exchange_rate FLOAT NOT NULL
);