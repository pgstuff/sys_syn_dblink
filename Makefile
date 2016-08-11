EXTENSION    = sys_syn_dblink
EXTVERSION   = $(shell grep default_version $(EXTENSION).control | sed -e "s/default_version[[:space:]]*=[[:space:]]*'\([^']*\)'/\1/")

DATA         = $(filter-out $(wildcard sql/*--*.sql),$(wildcard sql/*.sql))
DOCS         = $(wildcard doc/*.adoc doc/*.html)
TESTS        = $(wildcard test/sql/*.sql)
REGRESS      = $(patsubst test/sql/%.sql,%,$(TESTS))
REGRESS_OPTS = --inputdir=test --load-language=plpgsql
#
# Uncoment the MODULES line if you are adding C files
# to your extention.
#
#MODULES      = $(patsubst %.c,%,$(wildcard src/*.c))
PG_CONFIG    = pg_config
ASCIIDOC     = asciidoc

all: sql/$(EXTENSION)--$(EXTVERSION).sql doc-html-single

sql/$(EXTENSION)--$(EXTVERSION).sql: sql/$(EXTENSION).sql
	cp $< $@
#	sed -i 's/^-- EXT SELECT/SELECT/' $@

DATA = $(wildcard sql/*--*.sql) sql/$(EXTENSION)--$(EXTVERSION).sql
EXTRA_CLEAN = sql/$(EXTENSION)--$(EXTVERSION).sql doc/*.html

PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

doc-html-single:
ifeq (, $(shell which asciidoc 2> /dev/null))
	@echo "No asciidoc in $(PATH), install asciidoc for documentation in the HTML format"
	@echo "View the documentation at $(DESTDIR)$(docdir)/$(docmoduledir)/$(EXTENSION).adoc after the install (or doc/$(EXTENSION).adoc now)."
else
	$(ASCIIDOC) -a toc doc/$(EXTENSION).adoc
	@echo "View the documentation at $(DESTDIR)$(docdir)/$(docmoduledir)/$(EXTENSION).html after the install (or doc/$(EXTENSION).html now)."
endif
