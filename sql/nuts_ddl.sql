-- public."user" definition

-- Drop table

-- DROP TABLE public."user";

CREATE TABLE public."user" (
	user_id SERIAL NOT NULL,
	"name" varchar NULL,
	email varchar NULL,
	CONSTRAINT user_pk PRIMARY KEY (user_id)
);

-- public."order_table" definition

-- Drop table

-- DROP TABLE public."order_table";

CREATE TABLE public."order_table" (
	order_id SERIAL NOT NULL,
	user_id int4 NOT NULL,
	shape_layer varchar NULL,
	shape_roi varchar NULL,
	source_mission varchar NULL,
	source_data_type varchar NULL,
	start_time date NULL,
	stop_time date NULL,
	status varchar NULL,
	CONSTRAINT order_pk PRIMARY KEY (order_id)
);


-- public."order_table" foreign keys

ALTER TABLE public."order_table" ADD CONSTRAINT order_fk FOREIGN KEY (user_id) REFERENCES public."user"(user_id);

-- public.job definition

-- Drop table

-- DROP TABLE public.job;

CREATE TABLE public.job (
	job_id SERIAL NOT NULL,
	order_id int4 NOT NULL,
	start_date date NULL,
	end_date date NULL,
	status varchar NULL,
	CONSTRAINT job_pk PRIMARY KEY (job_id)
);


-- public.job foreign keys

ALTER TABLE public.job ADD CONSTRAINT job_fk FOREIGN KEY (order_id) REFERENCES public."order_table"(order_id);

-- public.source_product definition

-- Drop table

-- DROP TABLE public.source_product;

CREATE TABLE public.source_product (
	source_product_id SERIAL NOT NULL,
	job_id int4 NOT NULL,
	uri varchar NULL,
	product_name varchar NULL,
	status varchar NULL,
	CONSTRAINT source_product_pk PRIMARY KEY (source_product_id)
);


-- public.source_product foreign keys

ALTER TABLE public.source_product ADD CONSTRAINT source_product_fk FOREIGN KEY (job_id) REFERENCES public.job(job_id);
