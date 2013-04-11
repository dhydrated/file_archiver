file_archiver
=============

Usage: archiver.py [options]

Options:
  -h, --help            show this help message and exit
  -d directory, --directory=directory
                        directory where the logs are located
  -p pattern, --pattern=pattern
                        file pattern to process
  -i interval, --interval=interval
                        Period interval in day. Default is 1 day.
  -r, --remove          removed archived log
  -v, --verbose         print out messages


Example:
./archiver.py -d test-dir -p hello-*.csv -v
