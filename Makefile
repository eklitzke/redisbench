CFLAGS := -O3 -std=c++0x
LIBS := -lboost_program_options -lhiredis

redisbench: redisbench.cc
	$(CXX) $(CFLAGS) $< $(LIBS) -o $@

.PHONY: clean
clean:
	rm -f redisbench
