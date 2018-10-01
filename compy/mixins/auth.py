from compy.errors import UnauthorizedEvent
import hashlib
import re
import base64

__all__ = [
    "_AuthDatabaseMixin",
    "_BasicAuthDatabaseMixin"
]

class _AuthDatabaseMixin:
    def _validate_results(self, event, results):
        try:
            super(_AuthDatabaseMixin, self)._validate_results(event=event, results=results)
        except InvalidResultsException as e:
            event.status = 401
            event.environment["response"]["headers"].update({'WWW-Authenticate': 'Basic realm="Compysition Authentication"'})
            raise e

class _BasicAuthDatabaseMixin:

    def __extract_credentials(self, event):
        try:
            authorization = event.environment["request"]["headers"]['Authorization']
            tokens = authorization.strip().split(' ')
            basic_token = tokens[0]
            assert len(tokens) == 2
            assert basic_token == 'Basic'
            user, password = base64.decodestring(tokens[1]).split(':')
            return user, password
        except (AttributeError, KeyError):
            raise UnauthorizedEvent("No auth headers present in submitted request")
        except (AssertionError, ValueError, Exception):
            raise UnauthorizedEvent("Invalid auth headers present in submitted request")

    def __hash(self, raw):
        hasher = hashlib.sha256()
        hasher.update(raw)
        return hasher.hexdigest()

    def _get_dynamic_params(self, event):
        params = {}
        username, password = self.__extract_credentials(event=event)
        params["username"] = username
        params["password"] = self.__hash(raw=password)
        return [params]

    def _validate_results(self, event, results):
        target_methods = ["GET", "POST", "PUT", "DELETE"]
        for result in results:
            try:
                assert re.match(result["remote_address_regex"], event.environment.get("REMOTE_ADDR", ""))
                assert re.match(result["path_regex"], event.environment.get("PATH_INFO", ""))
                request_method = event.environment.get("REQUEST_METHOD", "")
                for method in target_methods:
                    assert result[method] if request_method == method else True
                assert result["other"] if request_method not in target_methods else True
                return
            except AssertionError:
                pass
        raise UnauthorizedEvent("User does not have adequate permissions")
