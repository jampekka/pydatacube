import urllib2
from pydatacube import pcaxis

from pydatacube.pcaxis import px_reader

PCAXIS_URL = "http://www.aluesarjat.fi/database/aluesarjat_kaupunkiverkko/tulotaso/asuntokuntien%20tulot/a01hki_asuntokuntien_tulot.px"
pcaxis_data = urllib2.urlopen(PCAXIS_URL)
cube = pcaxis.to_cube(pcaxis_data)
for row in cube:
	print "\t".join(map(unicode, row))
