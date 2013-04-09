#!/usr/bin/python


from optparse import OptionParser
import os
import glob
import time
import gzip
import datetime

class ArgumentParser:
	"""Commandline arguments"""

	options = ""
	args = ""

	def parse(self):
		self.parser = OptionParser()

		self.parser.add_option("-d", "--directory", dest="directory", metavar="directory",
                  help="directory where the logs are located")

		self.parser.add_option("-p", "--pattern", dest="pattern", metavar="pattern",
                  help="file pattern to process")

		self.parser.add_option("-i", "--interval", dest="interval", default=1, metavar="interval",
                  help="Period interval in day. Default is 1 day.")

		self.parser.add_option("-r", "--remove", action="store_true", dest="remove", default=False,
                  help="removed archived log")

		self.parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False,
                  help="print out messages")

		(self.options, self.args) = self.parser.parse_args()

		self.validate()

	def validate(self):
		if self.directory()==None:
			if self.pattern()==None:
				self.parser.print_help()
				exit()

	def directory(self):
		return self.options.directory

	def pattern(self):
		return self.options.pattern

	def verbose(self):
		return self.options.verbose

	def interval(self):
		return float(self.options.interval)

	def remove(self):
		return self.options.remove

	def printMe(self):
		print(self.options)
		print(self.args)

class Logger:
	"""Script logger"""

	def __init__(self, arguments):
		self.arguments = arguments
		self.verbose = self.arguments.verbose()

	def debug(self, msg):
		self.log('debug', msg)

	def info(self, msg):
		self.log('info', msg)

	def log(self, logLevel, msg):
		if logLevel == "debug" :
			if self.verbose :
				self._print_(msg)

		elif logLevel == "info" :
			self._print_(msg)
			
	def _print_(self,msg):
		print datetime.datetime.today()+' : '+msg


class LogsProcessor:
	"""processor"""

	def __init__(self, arguments):
		self.arguments = arguments
		self.logger = Logger(arguments)
		self.pattern = self.arguments.directory()+'/'+self.arguments.pattern()
		self.interval = self.arguments.interval()

	def execute(self):
		files = self._getFiles_()
				
		self.logger.debug(files)

		for filePath in files:
			fileObject = file(filePath, 'r')

			if self._isArchivable_(fileObject) :
				self.logger.debug("archiving: " + fileObject.name)
				self._archiveFile_(fileObject)
				if self._isRemovable_():
					self.logger.debug("deleting: " + fileObject.name)
					self._deleteFile_(fileObject)

	def _isRemovable_(self):
		return self.arguments.remove()

	def _isArchivable_(self, fileObject):
		fileLastModDate = self._getFileLastModDate_(fileObject)
		diffInDay = self._calcTimeDiffInDay_(fileLastModDate)
		return diffInDay > self.interval

	def _calcTimeDiffInDay_(self, fileLastModDate):
		now = time.time()
		diffInSecond =  now - fileLastModDate
		diffInDay = diffInSecond / 60 / 60 / 24
		return diffInDay

	def _getFileLastModDate_(self, fileObject):
		fileLastModDate =  os.path.getmtime(fileObject.name)
		fileObject.close
		return fileLastModDate

	def _archiveFile_(self, fileObject):
		f = gzip.open(fileObject.name+'.gz', 'wb', 9)
		f.write(fileObject.read())
		f.close()

	def _deleteFile_(self, fileObject):
		fileObject.close()
		os.remove(fileObject.name)

	def _getFiles_(self):
		return glob.glob(self.pattern)

def main():
	arguments = ArgumentParser()
	arguments.parse()
	
	processor = LogsProcessor(arguments)
	processor.execute()


if __name__ == "__main__":
	main()


	
