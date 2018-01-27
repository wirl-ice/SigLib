-- Query to create tblArchive - consistent with CIS archive shapefiles - specifies CIS lcc projection

-- Table: public.tblarchive

DROP TABLE IF EXISTS "public"."tblarchive";
CREATE TABLE public.tblarchive
(
  --ogc_fid integer NOT NULL DEFAULT nextval('tblarchive_ogc_fid_seq'::regclass),
  geom geometry(Polygon,96718),
  "catalog id" character(7),
  "obj type" character(2),
  subtype character(12),
  "valid time" character(19),
  "locked by" character(40),
  "product id" character(10),
  revision character(2),
  state character(8),
  distrib character(8),
  authors character(15),
  "desc" character(20),
  "orbit nbr" character(10),
  "beam mode" character(3),
  segment character(4),
  "leg nbr" character(4),
  bands character(4),
  "noaa ch3" character(1),
  enhance character(8),
  "cloud cvr" character(2),
  quality character(10),
  rows character(10),
  cols character(10),
  "pixel x" character(5),
  "pixel y" character(5),
  "xfr date" character(19),
  "cat date" character(19),
  "upd date" character(19),
  "cat by" character(15),
  "obj name" character(50),
  "file host" character(32),
  "file path" character(128),
  "file name" character(128),
  "dflt short" character(13),
  "dflt long" character(128),
  "file size" character(10),
  media character(5),
  projection character(30),
  lat1 character(8),
  lon1 character(8),
  lat2 character(8),
  lon2 character(8),
  lat3 character(8),
  lon3 character(8),
  lat4 character(8),
  lon4 character(8),
  "del date" character(19),
  "arch stat" character(2),
  "bg status" character(1),
  "bg type" character(8),
  "bg host" character(16),
  "bg path" character(128),
  "bg name" character(64),
  sortfld character(21),
  CONSTRAINT tblarchive_pkey PRIMARY KEY ("catalog id")
)
WITH (
  OIDS=FALSE
);

CREATE INDEX tblarchive_geom_idx
  ON public.tblarchive
  USING gist
  (geom );


