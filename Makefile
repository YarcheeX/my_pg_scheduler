MODULES = my_pg_scheduler

EXTENSION = my_pg_scheduler
DATA = my_pg_scheduler--1.0.sql
OBJS = my_pg_scheduler.o 

ifdef USE_PGXS
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
