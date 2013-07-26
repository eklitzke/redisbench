redisbench: redisbench.cc
	$(CXX) -std=c++0x $< -lboost_program_options -lhiredis -o $@

.PHONY: clean
clean:
	rm -f redisbench
