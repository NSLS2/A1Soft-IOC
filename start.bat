set EPICS_CA_AUTO_ADDR_LIST=NO
set EPICS_CAS_AUTO_BEACON_ADDR_LIST=NO
set EPICS_CA_ADDR_LIST={{ epics_subnet }}
set EPICS_CA_BEACON_ADDR_LIST={{ epics_subnet }}

pixi run python -m a1soft.ioc --list-pvs --prefix="{{ ioc.environment.PREFIX }}"
