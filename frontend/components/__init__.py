"""
UI Components for DataForge application

This package contains reusable UI components that follow consistent
patterns and inherit from BaseComponent for standardized behavior.
"""

from .base_component import BaseComponent
from .data_explorer import DataExplorer
from .plot_explorer import PlotExplorer
from .enhanced_plot_explorer import PlotExplorer as EnhancedPlotExplorer, PlotType, DataType
from .floating_data_explorer import FloatingDataExplorer

__all__ = [
    'BaseComponent', 
    'DataExplorer', 
    'PlotExplorer', 
    'EnhancedPlotExplorer',
    'PlotType',
    'DataType',
    'FloatingDataExplorer'
]