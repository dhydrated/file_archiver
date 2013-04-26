#!/usr/bin/python


from optparse import OptionParser
import os
import glob
import time
import gzip
import datetime
import logging

class Singleton(type):
    def __init__(self, *args, **kwargs):
        # Call the superclass (type), because we want Singleton isntances to
        # be intialised *mostly* the same as type isntances
        super(Singleton, self).__init__(*args, **kwargs)
        self.__instance = None

    def __call__(self, *args, **kwargs):
        # If self (the *class* object) has an __instance, return it. Otherwise
        # super-call __call__ to fall back to the normal class-call machinery
        # of calling the class' __new__ then __init__
        if self.__instance is None:
            self.__instance = super(Singleton, self).__call__(*args, **kwargs)
        return self.__instance

class ArgumentParser:
	"""Commandline arguments"""

	__metaclass__ = Singleton
	options = ""
	args = ""

	def __init__(self):
		self.parse()

	def parse(self):
		self.parser = OptionParser()

		self.parser.add_option("-d", "--directory", dest="directory", metavar="directory",
                  help="Directory where the logs are located. This field is required.")

		self.parser.add_option("-p", "--pattern", dest="pattern", metavar="pattern",
                  help="File pattern to process. This field is required.")

		self.parser.add_option("-i", "--interval", dest="interval", default=1, metavar="interval",
                  help="Period interval in day(s) based on the Last Modified Date of the file to be archived. Default is 1 day.")

		self.parser.add_option("-t", "--threshold", dest="threshold", default=100, metavar="threshold",
                  help="Period interval in day(s) based on the Last Modified Date of the archived files to be retained. Archived files Last Modified Date which are older will be deleted. Default is 100 days.")

		self.parser.add_option("-r", "--remove", action="store_true", dest="remove", default=False,
                  help="Remove files after archived. Default is false.")

		self.parser.add_option("-v", "--verbose", action="store_true", dest="verbose", default=False,
                  help="print out messages")

		(self.options, self.args) = self.parser.parse_args()

	def isValid(self):
		if (self.getDirectory() is None) | (self.getPattern() is None):
			return False
		else:
			return True

	def getDirectory(self):
		return self.options.directory

	def getPattern(self):
		return self.options.pattern

	def isVerbose(self):
		return self.options.verbose

	def getInterval(self):
		return float(self.options.interval)

	def getThreshold(self):
		return float(self.options.threshold)

	def isRemovable(self):
		return self.options.remove

	def printUsage(self):
		self.parser.print_help()

	def printMe(self):
		print(self.options)
		print(self.args)


class LoggerFactory:

	arguments = None

	@staticmethod
	def createLogger(name):
		logging.basicConfig(format='%(asctime)s %(name)s:%(lineno)s %(message)s', level=LoggerFactory._createLevel_(LoggerFactory._isVerbose_()))
		return logging.getLogger(name)
		
	@staticmethod
	def _createLevel_(verbose):
		level = logging.WARNING
		if verbose:
			level = logging.DEBUG

		return level

	@staticmethod
	def _isVerbose_():
		arguments = ArgumentParser()
		return arguments.isVerbose()


class BaseFileProcessor:
	"""base processor"""

	arguments = None
	pattern = None
	logger = None

	def _getFileLastModDate_(self, fileObject):
		fileLastModDate =  os.path.getmtime(fileObject.name)
		fileObject.close
		return fileLastModDate

	def _calcTimeDiffInDay_(self, fileLastModDate):
		now = time.time()
		diffInSecond =  now - fileLastModDate
		diffInDay = diffInSecond / 60 / 60 / 24
		return diffInDay

	def _deleteFile_(self, fileObject):
		self._getLogger_().debug("deleting: " + fileObject.name)
		fileObject.close()
		os.remove(fileObject.name)

	def _getFiles_(self):
		return glob.glob(self._getPattern__())

	def execute(self):
		pass

	def _getArguments_(self):
		if self.arguments == None:
			self.arguments = ArgumentParser()
		return self.arguments

	def _getPattern__(self):
		return self.pattern

	def _getLogger_(self):
		if self.logger == None:
			self.logger = LoggerFactory.createLogger(self.__class__.__name__)
		return self.logger


class ArchivedLogsProcessor(BaseFileProcessor):
	"""archived logs processor"""

	logger = None
	threshold = None

	def __init__(self):
		self.pattern = self._getArguments_().getDirectory()+'/'+self._getArguments_().getPattern()+'.gz'

	def execute(self):
		files = self._getFiles_()
				
		for filePath in files:
			fileObject = file(filePath, 'r')

			if self._isRemovable_(fileObject) :
				self._getLogger_().debug("deleting: " + fileObject.name)
				self._deleteFile_(fileObject)

	def _isRemovable_(self, fileObject):
		fileLastModDate = self._getFileLastModDate_(fileObject)
		diffInDay = self._calcTimeDiffInDay_(fileLastModDate)
		return diffInDay > self._getThreshold__()

	def _getThreshold__(self):
		if self.threshold is None:
			self.threshold = self._getArguments_().getThreshold()
		return self.threshold

	def _getLogger_(self):
		if self.logger == None:
			self.logger = LoggerFactory.createLogger(self.__class__.__name__)
		return self.logger


class LogsProcessor(BaseFileProcessor):
	"""logs processor"""

	interval = None
	removable = None
	logger = None

	def __init__(self):
		self.pattern = self._getArguments_().getDirectory()+'/'+self._getArguments_().getPattern()
		
	def execute(self):
		files = self._getFiles_()
				
		for filePath in files:
			fileObject = file(filePath, 'r')

			if self._isArchivable_(fileObject) :
				self._archiveFile_(fileObject)
				if self._isRemovable_():
					self._deleteFile_(fileObject)

	def _isRemovable_(self):
		if self.removable is None:
			self.removable = self._getArguments_().isRemovable()
		return self.removable


	def _getInterval_(self):
		if self.interval is None:
			self.interval = self._getArguments_().getInterval()
		return self.interval

	def _isArchivable_(self, fileObject):
		fileLastModDate = self._getFileLastModDate_(fileObject)
		diffInDay = self._calcTimeDiffInDay_(fileLastModDate)
		return diffInDay > self._getInterval_()

	def _archiveFile_(self, fileObject):
		self._getLogger_().debug("archiving: " + fileObject.name + ' to ' + fileObject.name + '.gz')
		f = gzip.open(fileObject.name+'.gz', 'wb', 9)
		f.write(fileObject.read())
		f.close()

	def _getLogger_(self):
		if self.logger == None:
			self.logger = LoggerFactory.createLogger(self.__class__.__name__)
		return self.logger

def main():
	arguments = ArgumentParser()

	if arguments.isValid():
		logsProcessor = LogsProcessor()
		logsProcessor.execute()
		archivedLogsProcessor = ArchivedLogsProcessor()
		archivedLogsProcessor.execute()
		
	else:
		arguments.printUsage()


if __name__ == "__main__":
	main()


	
