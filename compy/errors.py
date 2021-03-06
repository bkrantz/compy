#!/usr/bin/env python

class CompysitionException(Exception):

    def __init__(self, message="", **kwargs):
        if not isinstance(message, list):
            message = [message]
        self.__dict__.update(kwargs)
        super(CompysitionException, self).__init__(message)

class QueueEmpty(CompysitionException):
    pass

class QueueFull(CompysitionException):
    def __init__(self, queue=None, **kwargs):
        self.queue = queue
        super(QueueFull, self).__init__(**kwargs)

class QueueConnected(CompysitionException):
    pass


class SetupError(CompysitionException):
    pass


class ReservedName(CompysitionException):
    pass


class ActorInitFailure(CompysitionException):
    """**A specific Actor initialization failed**"""
    pass


class InvalidEventConversion(CompysitionException):
    """**An attempt to convert Event or an Event subclass to a different class was impossible**"""
    pass


class InvalidEventDataModification(CompysitionException):
    """**An attempt to modify Event.data to an invalid format for the Event subclass occurred**"""
    pass

class InvalidEventModification(CompysitionException):
    pass


class InvalidActorOutput(CompysitionException):
    """**An Actor sent an event that was not defined as a valid output Event class**"""
    pass


class InvalidActorInput(CompysitionException):
    """**An Actor received an event that was not defined as a valid input Event class**"""
    pass


class ResourceNotModified(CompysitionException):
    """**An Actor attempted to modify an external persistent entity, but that modification was not successful**"""
    pass


class MalformedEventData(CompysitionException):
    """**Event data was malformed for a particular Actor to work on it**"""
    pass


class UnauthorizedEvent(CompysitionException):
    """**Event either did not contain credentials for a restricted actor, or contained invalid credentials**"""
    pass


class ForbiddenEvent(CompysitionException):
    """**Event was authenticated properly for a given Actor, but the credentials were not granted permissions for the requested action**"""
    pass


class ResourceNotFound(CompysitionException):
    """**A specific queue was requested on an Actor, but that queue was not defined or connected for that actor**"""
    pass


class EventCommandNotAllowed(CompysitionException):
    """**A semantic property on an incoming event that modified actor behavior was not implemented**"""
    pass


class ActorTimeout(CompysitionException):
    """**An Actor timed out the requested work on an incoming event**"""
    pass


class ResourceConflict(CompysitionException):
    """**An event attempted to draw upon a persistant storage resource that is no longer available**"""
    pass


class ResourceGone(CompysitionException):
    """**An event attempted to draw upon a persistant storage resource that is no longer available**"""
    pass


class UnprocessableEventData(CompysitionException):
    """**Event data was well formed, but unprocessable for application logic semantic purposes**"""
    pass


class EventRateExceeded(CompysitionException):
    """**The allowed event rate for a given actor has been exceeded**"""
    pass


class ServiceUnavailable(CompysitionException):
    """**A defined Event.service was requested but that particular service was not found**"""
    pass


class EventAttributeError(CompysitionException):
    """**An event attribute necessary to the proper processing of the event was missing**"""
    pass