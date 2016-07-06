all: burst.o
	gcc burst.o -o burst -lpthread

burst.o: burst.c
	gcc -c burst.c
clean:
	rm -rf *.o
	rm -f burst

install:
	cp burst /usr/bin/	
	(cat burst.man |gzip > /usr/local/man/man1/burst.1 2>/dev/null) || (cat burst.man |gzip > /usr/local/share/man/man1/burst.1 2>/dev/null)

uninstall:
	rm /usr/bin/burst
	(rm /usr/local/man/man1/burst.1) || (rm /usr/local/share/man/man1/burst.1)
