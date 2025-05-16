from configparser import ConfigParser
from typing import List, Optional, Any, Dict


class Config:
    def __init__(self, config_file: str = 'config.cfg'):
        self.cfg = ConfigParser()
        self.cfg.read(config_file)
        self._overrides: Dict[str, Any] = {}

    def override(self, key: str, value: Any) -> None:
        self._overrides[key] = value

    def set_all_pairs(self, value: bool) -> None:
        self._overrides['all_pairs'] = value

    @property
    def exchange_name(self) -> str:
        return self.cfg.get('DEFAULT', 'exchange_name')

    @property
    def all_pairs(self) -> bool:
        return self._overrides.get('all_pairs', self.cfg.getboolean('DEFAULT', 'all_pairs'))

    @property
    def base_symbols(self) -> List[str]:
        return self._overrides.get('base_symbols', self.cfg.get('DEFAULT', 'base_symbols').split(','))

    @property
    def quote_symbols(self) -> List[str]:
        return self._overrides.get('quote_symbols', self.cfg.get('DEFAULT', 'quote_symbols').split(','))

    @property
    def timeframes(self) -> List[str]:
        return self._overrides.get('timeframes', self.cfg.get('DEFAULT', 'timeframes').split(','))

    @property
    def start_time(self) -> str:
        return self.cfg.get('DEFAULT', 'start_time')

    @property
    def end_time(self) -> Optional[str]:
        return self.cfg.get('DEFAULT', 'end_time') or None

    @property
    def batch_size(self) -> int:
        return self.cfg.getint('DEFAULT', 'batch_size')

    @property
    def output_directory(self) -> str:
        return self.cfg.get('DEFAULT', 'output_directory')

    @property
    def output_file(self) -> Optional[str]:
        return self.cfg.get('DEFAULT', 'output_file') or None

    @property
    def log_to_file(self) -> bool:
        return self.cfg.getboolean('DEFAULT', 'enable_logging')
