
__all__ = [
	"InvalidResultsException"
]

class InvalidResultsException(Exception):
	def __init__(self, expected, received, *args, **kwargs):
		message = "Query returned unexpected results. Expected {expected} but received {received}".format(
			expected=expected, received=received)
		self.expected = expected
		self.received = received
		super(InvalidResultsException, self).__init__(message, *args, **kwargs)