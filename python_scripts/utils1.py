text_file = open('/Users/jradioac/Desktop/data/sql_scripts/1.sql', 'r')
writetofile = open('/Users/jradioac/Desktop/data/sql_scripts/2.sql', 'w')

lines = text_file.read().split(';\n')

for i in range( 0, len(lines)):
	writetofile.write('curs.execute( """' + lines[i] + '""")\n')

# print("Hello", end=' ')
# print("World")
