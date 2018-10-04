class _RequestHeaderDatabaseMixin(_DatabaseMixin, LookupMixin):
	def _get_dynamic_params(self, event):
		param_groups = []
		for header, value in event.environment["request"]["headers"].iteritems():
			param_groups.append({
				"meta_id": event.meta_id,
				"request_header": header,
				"request_header_value": value
			})
		return param_groups

	def _format_results(self, event, results):
		return []

class _ResponseHeaderDatabaseMixin(_DatabaseMixin, LookupMixin):
	def _get_dynamic_params(self, event):
		param_groups = []
		for header, value in event.environment["response"]["headers"].iteritems():
			param_groups.append({
				"meta_id": event.meta_id,
				"response_header": header,
				"response_header_value": value
			})
		return param_groups

	def _format_results(self, event, results):
		return []

class _RequestDatabaseMixin(_DatabaseMixin, LookupMixin):
	def _get_dynamic_params(self, event):
		params = {
			"meta_id": event.meta_id,
			"service": event.service,
			"client_ip": event.environment["remote"]["address"],
			"path": event.environment["request"]["path"],
			"method": event.environment["request"]["method"],
			"request_body": event.data
		}
		return [params]

	def _format_results(self, event, results):
		return []

class _ResponseDatabaseMixin(_DatabaseMixin, LookupMixin):
	def _get_dynamic_params(self, event):
		params = {
			"meta_id": event.meta_id,
			"response_body": event.data,
			"response_code": event.status
		}
		return [params]

	def _format_results(self, event, results):
		return []

class RequestHeaderMySQLWriteActor(_RequestHeaderDatabaseMixin, _MySQLMixin, _MySQLWriteMixin, _MySQLAutoMixin, _DatabaseAuto):
	pass

class RequestMySQLWriteActor(_RequestDatabaseMixin, _MySQLMixin, _MySQLWriteMixin, _MySQLAutoMixin, _DatabaseAuto):
	pass

class ResponseHeaderMySQLWriteActor(_ResponseHeaderDatabaseMixin, _MySQLMixin, _MySQLWriteMixin, _MySQLAutoMixin, _DatabaseAuto):
	pass

class ResponseMySQLWriteActor(_ResponseDatabaseMixin, _MySQLMixin, _MySQLWriteMixin, _MySQLAutoMixin, _DatabaseAuto):
	pass