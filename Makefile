redisbench: redisbench.cc
	$(CXX) -std=c++0x $< -lhiredis -o $@

.PHONY: clean
clean:
	rm -f redisbench
