"""Quick and dirty literal programming tool

Converts commented Python-code to markdown.
"""
import sys
from itertools import chain
from StringIO import StringIO

def paragraphs(lines):
	para = []
	for line in lines:
		line = line.strip('\n')
		if line.strip() == '':
			for line in lines:
				if line.strip() != '':
					lines = chain(line, lines)
					break
			yield ' '.join(para)
			para = []
		else:
			para.append(line)
	yield ' '.join(para)

def qndlit(lines=sys.stdin, output=sys.stdout):
	def parse_comments(lines):
		buf = []
		for line in lines:
			if line.strip() == '':
				buf.append(line)
				continue
			if not line.startswith('# '):
				lines = chain([line], lines)
				break
			line = line[2:]
			buf.append(line)
		else:
			lines = []

		for paragraph in paragraphs(buf):
			output.write(paragraph+"\n\n")
		return lines
	
	nestlocals, nestglobals = {}, {}
	exec 'import sys' in nestlocals, nestglobals
	def parse_code(lines):
		buf = []
		for line in lines:
			if line.strip() == '':
				buf.append(line)
				continue
			if line.startswith('# '):
				lines = chain([line], lines)
				break
			buf.append(line)
		else:
			lines = []

		if buf[-1].strip() == '':
			lines = chain(buf[-1], lines)
			buf = buf[:-1]
		
		code = ''.join(buf)
		
		sys.stdout = StringIO()
		exec code in nestlocals, nestglobals
		nestoutput = sys.stdout.getvalue()
		sys.stdout = sys.__stdout__


		output.write('```python\n')
		output.write(code)
		output.write('```\n')
		
		if nestoutput:
			output.write('```\n')
			output.write(nestoutput)
			output.write('```\n')

		return lines


			
	parsers = [parse_comments, parse_code]

	try:
		while True:
			for parser in parsers:
				lines = parser(lines)
				if lines == []:
					break
			if lines == []:
				break
	except StopIteration:
		pass
		

if __name__ == '__main__':
	qndlit()
