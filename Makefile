CFLAGS := -O3 -std=c++0x -pthread
LIBS := -lboost_program_options -lboost_system

redisbench: redisbench.cc
	$(CXX) $(CFLAGS) $< $(LIBS) -o $@

.PHONY: clean
clean:
	rm -f redisbench
