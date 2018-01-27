-- Table: public.tblroi

DROP TABLE public.tblroi;

CREATE TABLE public.tblroi
(
  id double precision,
  obj character varying(5),
  instid character varying(3),
  todate character varying(19),
  fromdate character varying(19),
  notes character varying(50),
  area character varying(30),
  geom geometry(Polygon,96718)
  
)
WITH (
  OIDS=FALSE
);

CREATE INDEX tblroi_geom_gist ON public.tblroi  USING gist  (geom );

