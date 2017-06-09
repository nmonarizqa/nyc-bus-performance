SERVER = http://data.mytransit.nyc/bus_time
.PHONY: calls-$(DAY) calls-%

calls-$(DAY): calls-%

calls-%: calls/%.csv.xz
	unxz $<

calls/%.csv.xz: | calls
	curl -o $@ $(SERVER)/$(word 1,$(subst -, ,$*))/$(word 1,$(subst -, ,$*))-$(word 2,$(subst -, ,$*))/bus_time_$(subst -,,$*).csv.xz
