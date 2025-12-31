"""Core interfaces package for the frontend application."""

from .icontroller import IController
from .iview import IView
from .irepository import IRepository

__all__ = ['IController', 'IView', 'IRepository']
