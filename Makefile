test: 
	py.test --doctest-modules --cover=datalib

test_html_coverage:
	py.test --doctest-modules --cover-report=html --cover=datalib

