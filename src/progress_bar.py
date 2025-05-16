from datetime import timedelta
import os
import sys
import time

class ProgressBar:
    def __init__(self, total: int, prefix: str = '', length: int = 30):
        self.total = max(1, total) 
        self.prefix = prefix
        self.length = length
        self.current = 0
        self.start_time = time.time()
        self.last_update_time = 0
        self.update_interval = 0.2
        
    def update(self, current: int) -> None:
        self.current = min(current, self.total)

        current_time = time.time()
        if current_time - self.last_update_time < self.update_interval and current < self.total:
            return
            
        self.last_update_time = current_time
        progress = self.current / self.total
        blocks = int(self.length * progress)
        bar = '█' * blocks + '░' * (self.length - blocks)
        elapsed = current_time - self.start_time
        
        if progress > 0:
            eta = (elapsed / progress) * (1 - progress)
            eta_str = f"ETA: {timedelta(seconds=int(eta))}"
        else:
            eta_str = "ETA: calculating..."
        
        output = f"\r{self.prefix} |{bar}| {int(progress*100)}% {current}/{self.total} {eta_str}"

        terminal_width = self._get_terminal_width()
        if terminal_width > 0:
            padding = ' ' * max(0, terminal_width - len(output) - 5)
            output += padding
            
        sys.stdout.write(output)
        sys.stdout.flush()
        
    def finish(self) -> None:
        elapsed = time.time() - self.start_time
        output = f"\r{self.prefix} |{'█' * self.length}| 100% {self.total}/{self.total} Complete in {timedelta(seconds=int(elapsed))}"
        
        # Add padding and newline
        terminal_width = self._get_terminal_width()
        if terminal_width > 0:
            padding = ' ' * max(0, terminal_width - len(output) - 5)
            output += padding
            
        sys.stdout.write(output + "\n")
        sys.stdout.flush()
    
    def _get_terminal_width(self) -> int:
        """Get terminal width safely"""
        try:
            terminal_size = os.get_terminal_size()
            return terminal_size.columns
        except (AttributeError, OSError, ImportError):
            return 80  # Default fallback width
