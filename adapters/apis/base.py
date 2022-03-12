"""
Abstract adapter to standardize function calls
"""
from abc import ABC, abstractmethod

class BaseAdapter(ABC):
    @abstractmethod
    def get(self):
        return NotImplemented
