../README.md: README.hdr.md intro.md
	cat README.hdr.md intro.md > ../README.md

intro.md: ../examples/intro.py qndlit.py
	python2 qndlit.py < ../examples/intro.py > intro.md
