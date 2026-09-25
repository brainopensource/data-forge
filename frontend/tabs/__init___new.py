"""
Tab modules for DataForge application
"""

from .base_tab import BaseTab
from .home_tab import HomeTab
from .database_tab_new import DatabaseTab
from .exploration_tab_new import ExplorationTab
from .plugins_tab_new import PluginsTab
from .help_tab_new import HelpTab

__all__ = [
    'BaseTab',
    'HomeTab', 
    'DatabaseTab',
    'ExplorationTab',
    'PluginsTab',
    'HelpTab'
]
